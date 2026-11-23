// Despliega la app de ventas (Kafka Streams + productor) en su EC2 sin SSH: sube a S3 el contenido de app/
// (docker-compose.yml, .env, Dockerfile, pom.xml, src/; sin target/) y vía SSM (SendCommand) la instancia
// lo descarga del bucket (instance profile), construye la imagen y ejecuta `docker compose up -d`.
// Se ejecuta desde la máquina local, con Kafka ya desplegado (deploy-kafka.js): init-topics reintenta
// hasta que los brokers acepten, pero sin brokers la app no arranca.
//
// También sube los scripts de scripts/ec2/*.sh (p. ej. demo-duplicados.sh) y los deja en remoteDir con
// permiso de ejecución. Solo los copia: este despliegue nunca los ejecuta; se corren a mano dentro de la EC2.
//
// Antes de subir avisa si ya hay copias en el bucket (se sobrescriben). Al terminar, con éxito o error,
// borra del bucket los objetos bajo app/ (el .env puede llevar configuración sensible).
//
// Uso:  node deploy-app.js   (lee scripts/.env; la shell tiene prioridad sobre el archivo)
// Env:  S3_BUCKET (obligatoria, debe existir) | AWS_REGION | APP_REMOTE_DIR | APP_INSTANCE_NAME
//       + credenciales AWS del Learner Lab (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)

import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { DescribeInstancesCommand, EC2Client } from "@aws-sdk/client-ec2";
import { DeleteObjectsCommand, ListObjectsV2Command, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { GetCommandInvocationCommand, SendCommandCommand, SSMClient, waitUntilCommandExecuted } from "@aws-sdk/client-ssm";

// Relativo al script (no al cwd); loadEnvFile no pisa variables ya definidas en la shell
const envFile = fileURLToPath(new URL(".env", import.meta.url));
if (existsSync(envFile)) process.loadEnvFile(envFile);

const region = process.env.AWS_REGION ?? "us-east-1";
const bucket = process.env.S3_BUCKET;
const remoteDir = process.env.APP_REMOTE_DIR ?? "/opt/ventas";
const instanceName = process.env.APP_INSTANCE_NAME ?? "act3-app";

if (!bucket) {
  console.error("Falta S3_BUCKET (nombre del bucket donde se suben los archivos de app/).");
  process.exit(1);
}

// El prefijo lleva la barra final: "app/" no coincide con "app-1/" ni "app-2/" de deploy-kafka.js
const prefix = "app/";
const appDir = fileURLToPath(new URL("../app/", import.meta.url));
// Artefactos de build: la imagen se compila en la EC2
const excluded = new Set(["target"]);
// Scripts para correr dentro de la EC2: viajan por el mismo prefijo app/ para que `s3 sync` los deje en remoteDir
const ec2ScriptsDir = fileURLToPath(new URL("ec2/", import.meta.url));

const ec2 = new EC2Client({ region });
const s3 = new S3Client({ region });
const ssm = new SSMClient({ region });

// Archivos de app/ (rutas relativas con "/"), sin los directorios excluidos
function localFiles() {
  return readdirSync(appDir, { recursive: true })
    .map((p) => p.replaceAll("\\", "/"))
    .filter((p) => !p.split("/").some((part) => excluded.has(part)))
    .filter((p) => statSync(join(appDir, p)).isFile());
}

// Nombres de los .sh de scripts/ec2/ (vacío si la carpeta no existe)
function ec2Scripts() {
  if (!existsSync(ec2ScriptsDir)) return [];
  return readdirSync(ec2ScriptsDir).filter((f) => f.endsWith(".sh") && statSync(join(ec2ScriptsDir, f)).isFile());
}

// Claves existentes bajo app/ (incluye el marcador de carpeta "app/" si lo hay)
async function listKeys() {
  const keys = [];
  let ContinuationToken;
  do {
    const page = await s3.send(new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix, ContinuationToken }));
    keys.push(...(page.Contents ?? []).map((o) => o.Key));
    ContinuationToken = page.NextContinuationToken;
  } while (ContinuationToken);
  return keys;
}

async function upload() {
  const files = localFiles();
  for (const required of ["docker-compose.yml", ".env", "Dockerfile", "pom.xml"]) {
    if (!files.includes(required)) throw new Error(`No existe ${appDir}${required}`);
  }

  const existing = await listKeys().catch((err) => {
    if (err.name === "NoSuchBucket") throw new Error(`El bucket ${bucket} no existe en ${region}.`);
    throw err;
  });
  if (existing.length) console.log(`Ya existen ${existing.length} objetos en s3://${bucket}/${prefix}; se sobrescriben.`);

  for (const file of files) {
    const key = `${prefix}${file}`;
    await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: readFileSync(join(appDir, file)) }));
    console.log(`Subido s3://${bucket}/${key}`);
  }

  for (const name of ec2Scripts()) {
    if (files.includes(name)) throw new Error(`scripts/ec2/${name} choca con app/${name}; renombra uno de los dos.`);
    // LF forzado: con core.autocrlf el .sh podría estar en CRLF y bash fallaría en Linux
    const body = readFileSync(join(ec2ScriptsDir, name), "utf8").replaceAll("\r\n", "\n");
    await s3.send(new PutObjectCommand({ Bucket: bucket, Key: `${prefix}${name}`, Body: body }));
    console.log(`Subido s3://${bucket}/${prefix}${name} (script para la EC2; no se ejecuta)`);
  }
}

// El ID de la instancia cambia en cada despliegue: se busca por tag Name.
async function findInstanceId() {
  const { Reservations } = await ec2.send(
    new DescribeInstancesCommand({
      Filters: [
        { Name: "tag:Name", Values: [instanceName] },
        { Name: "instance-state-name", Values: ["running"] },
      ],
    }),
  );
  const instance = Reservations?.flatMap((r) => r.Instances ?? [])[0];
  if (!instance) throw new Error(`No hay una instancia running con tag Name=${instanceName}`);
  return instance.InstanceId;
}

async function deploy() {
  const instanceId = await findInstanceId();
  console.log(`\n== ${instanceName} (${instanceId}) ==`);

  const commands = [
    "set -eux",
    // SSM ejecuta como root sin HOME; aws cli y docker compose lo necesitan
    "export HOME=/root",
    `mkdir -p ${remoteDir}`,
    // --delete: un archivo borrado en app/ no debe sobrevivir en el build de la EC2
    `aws s3 sync s3://${bucket}/${prefix} ${remoteDir}/ --delete --region ${region}`,
    `chmod 600 ${remoteDir}/.env`,
    // S3 no conserva el bit de ejecución; los scripts de scripts/ec2/ solo se dejan listos, no se ejecutan
    ...ec2Scripts().map((name) => `chmod +x ${remoteDir}/${name}`),
    `cd ${remoteDir}`,
    // No `docker compose pull` a secas: ventas-app:latest se construye aquí y no existe en ningún registry
    "docker compose pull init-topics",
    "docker compose up -d --build",
    "docker compose ps -a",
    // Muestra el arranque de Streams y del productor (si fallan, quedan en la salida del comando)
    "sleep 30",
    "docker compose logs --tail 20 consumer producer",
  ];

  const { Command } = await ssm.send(
    new SendCommandCommand({
      InstanceIds: [instanceId],
      DocumentName: "AWS-RunShellScript",
      Comment: "Despliega la app de ventas desde S3",
      Parameters: { commands },
      TimeoutSeconds: 1800,
    }),
  );
  const commandId = Command.CommandId;
  console.log(`SSM command ${commandId} enviado; esperando resultado...`);

  // Falla (lanza) si el comando termina en Failed/TimedOut; el detalle se imprime abajo en cualquier caso
  let waitError;
  try {
    await waitUntilCommandExecuted({ client: ssm, maxWaitTime: 1800 }, { CommandId: commandId, InstanceId: instanceId });
  } catch (err) {
    waitError = err;
  }

  const result = await ssm.send(new GetCommandInvocationCommand({ CommandId: commandId, InstanceId: instanceId }));
  console.log(`Estado: ${result.Status}`);
  if (result.StandardOutputContent) console.log(`--- stdout ---\n${result.StandardOutputContent}`);
  if (result.StandardErrorContent) console.error(`--- stderr ---\n${result.StandardErrorContent}`);

  if (result.Status !== "Success") {
    throw waitError ?? new Error(`El comando terminó con estado ${result.Status}`);
  }
}

// Borra todo lo que haya bajo app/ (archivos y marcadores de carpeta); no toca el resto del bucket.
async function cleanup() {
  const keys = await listKeys();
  // DeleteObjects admite hasta 1000 claves por llamada
  for (let i = 0; i < keys.length; i += 1000) {
    const batch = keys.slice(i, i + 1000);
    await s3.send(new DeleteObjectsCommand({ Bucket: bucket, Delete: { Objects: batch.map((Key) => ({ Key })), Quiet: true } }));
  }
  if (keys.length) console.log(`Borrados ${keys.length} objetos de s3://${bucket}/${prefix}`);
}

async function main() {
  try {
    await upload();
    await deploy();
    console.log("\nListo: app de ventas desplegada.");
    for (const name of ec2Scripts()) {
      console.log(`Script disponible en la EC2 (no ejecutado): sudo bash ${remoteDir}/${name}`);
    }
  } finally {
    // No debe enmascarar el error original del despliegue
    await cleanup().catch((err) => console.error(`AVISO: no se pudo limpiar el bucket (${err.message}); borra ${prefix} a mano.`));
  }
}

main().catch((err) => {
  console.error(err.message ?? err);
  process.exit(1);
});
