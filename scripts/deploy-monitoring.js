// Despliega el monitoreo sin SSH, en dos fases (mismo patrón S3 + SSM que deploy-kafka.js):
//   1. Stack Prometheus + Loki + Grafana en app-monitoring: sube monitoring/ (compose, prometheus, loki,
//      provisioning de Grafana y monitoring.env) a s3://$S3_BUCKET/monitoring/ y lo levanta con docker compose.
//   2. Grafana Alloy (logs de contenedores -> Loki) y node_exporter (CPU/RAM/disco de la EC2, puerto 9100, que
//      scrapea Prometheus) en app-monitoring, app-zookeper, app-broker-0 y app: sube docker-compose.alloy.yaml
//      y alloy/config.alloy a s3://$S3_BUCKET/alloy/ y escribe en cada nodo un alloy.env con su NODE_NAME y la
//      URL de Loki.
// Se ejecuta desde la máquina local, tras deploy-kafka.js y deploy-app.js (los exporters JMX ya deben
// estar activos para que Prometheus tenga qué scrapear).
//
// Requiere monitoring/monitoring.env (copiar de monitoring.env.example; está fuera de git). Al terminar,
// con éxito o error, borra del bucket los objetos bajo monitoring/ y alloy/ (monitoring.env lleva la
// contraseña de Grafana).
//
// Uso:  node deploy-monitoring.js   (lee scripts/.env; la shell tiene prioridad sobre el archivo)
// Env:  S3_BUCKET (obligatoria, debe existir) | AWS_REGION | MONITORING_REMOTE_DIR | ALLOY_REMOTE_DIR | LOKI_URL
//       | MONITORING_INSTANCE_NAME | ZOOKEEPER_INSTANCE_NAME | BROKER_INSTANCE_NAME | APP_INSTANCE_NAME
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
const monitoringDir = process.env.MONITORING_REMOTE_DIR ?? "/opt/monitoring";
const alloyDir = process.env.ALLOY_REMOTE_DIR ?? "/opt/alloy";
const monitoringInstanceName = process.env.MONITORING_INSTANCE_NAME ?? "act3-app-monitoring";

if (!bucket) {
  console.error("Falta S3_BUCKET (nombre del bucket donde se suben los archivos de monitoring/).");
  process.exit(1);
}

const localDir = fileURLToPath(new URL("../monitoring/", import.meta.url));
const envPath = join(localDir, "monitoring.env");

// Los prefijos llevan la barra final: no coinciden con app-1/, app-2/ ni app/ de los otros scripts
const stack = {
  prefix: "monitoring/",
  files: [
    "docker-compose.monitoring.yaml",
    "prometheus.yml",
    "prometheus-entrypoint.sh",
    "loki.yaml",
    "monitoring.env",
    ...readdirSync(join(localDir, "grafana"), { recursive: true })
      .map((p) => `grafana/${p.replaceAll("\\", "/")}`)
      .filter((p) => statSync(join(localDir, p)).isFile()),
  ],
};
const alloy = { prefix: "alloy/", files: ["docker-compose.alloy.yaml", "alloy/config.alloy"] };

// Nodos con Alloy (NODE_NAME = label "node" de los logs). Monitoring va primero: Loki debe estar arriba.
const alloyNodes = [
  { nodeName: "monitoring", instanceName: monitoringInstanceName },
  { nodeName: "zookeper", instanceName: process.env.ZOOKEEPER_INSTANCE_NAME ?? "act3-app-zookeper" },
  { nodeName: "broker-0", instanceName: process.env.BROKER_INSTANCE_NAME ?? "act3-app-broker-0" },
  { nodeName: "app", instanceName: process.env.APP_INSTANCE_NAME ?? "act3-app" },
];

const ec2 = new EC2Client({ region });
const s3 = new S3Client({ region });
const ssm = new SSMClient({ region });

// Lee KEY=valor de monitoring.env (sin comillas ni expansión, como lo interpreta docker compose para estos valores)
function readEnv(path) {
  const values = {};
  for (const line of readFileSync(path, "utf8").split(/\r?\n/)) {
    const match = line.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$/);
    if (match) values[match[1]] = match[2];
  }
  return values;
}

function checkEnv() {
  if (!existsSync(envPath)) throw new Error(`No existe ${envPath}: cópialo de monitoring.env.example y completa los valores.`);
  const env = readEnv(envPath);
  for (const key of ["GRAFANA_ADMIN_PASSWORD", "GRAFANA_DOMAIN", "MONITORING_PRIVATE_IP", "KAFKA_1_PRIVATE_IP", "KAFKA_2_PRIVATE_IP", "ZOOKEEPER_PRIVATE_IP", "APP_PRIVATE_IP"]) {
    if (!env[key]) throw new Error(`Falta ${key} en ${envPath}`);
  }
  if (env.GRAFANA_ADMIN_PASSWORD.startsWith("replace-with")) {
    throw new Error(`GRAFANA_ADMIN_PASSWORD en ${envPath} sigue con el valor de ejemplo; cámbialo.`);
  }
  return env;
}

// Claves existentes bajo el prefijo (incluye el marcador de carpeta si lo hay)
async function listKeys(prefix) {
  const keys = [];
  let ContinuationToken;
  do {
    const page = await s3.send(new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix, ContinuationToken }));
    keys.push(...(page.Contents ?? []).map((o) => o.Key));
    ContinuationToken = page.NextContinuationToken;
  } while (ContinuationToken);
  return keys;
}

async function upload({ prefix, files }) {
  for (const file of files) {
    if (!existsSync(join(localDir, file))) throw new Error(`No existe ${join(localDir, file)}`);
  }
  const existing = await listKeys(prefix).catch((err) => {
    if (err.name === "NoSuchBucket") throw new Error(`El bucket ${bucket} no existe en ${region}.`);
    throw err;
  });
  if (existing.length) console.log(`Ya existen ${existing.length} objetos en s3://${bucket}/${prefix}; se sobrescriben.`);

  for (const file of files) {
    await s3.send(new PutObjectCommand({ Bucket: bucket, Key: `${prefix}${file}`, Body: readFileSync(join(localDir, file)) }));
    console.log(`Subido s3://${bucket}/${prefix}${file}`);
  }
}

// El ID de la instancia cambia en cada despliegue: se busca por tag Name.
async function findInstanceId(instanceName) {
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

async function runOn(instanceName, comment, commands) {
  const instanceId = await findInstanceId(instanceName);
  console.log(`\n== ${instanceName} (${instanceId}) ==`);

  const { Command } = await ssm.send(
    new SendCommandCommand({
      InstanceIds: [instanceId],
      DocumentName: "AWS-RunShellScript",
      Comment: comment,
      Parameters: { commands },
      TimeoutSeconds: 900,
    }),
  );
  const commandId = Command.CommandId;
  console.log(`SSM command ${commandId} enviado; esperando resultado...`);

  // El waiter lanza (con el JSON completo de la invocación) si el comando termina en Failed/TimedOut;
  // se ignora y se informa abajo con stdout/stderr y el estado, que es lo útil.
  await waitUntilCommandExecuted({ client: ssm, maxWaitTime: 900 }, { CommandId: commandId, InstanceId: instanceId }).catch(() => {});

  const result = await ssm.send(new GetCommandInvocationCommand({ CommandId: commandId, InstanceId: instanceId }));
  console.log(`Estado: ${result.Status}`);
  if (result.StandardOutputContent) console.log(`--- stdout ---\n${result.StandardOutputContent}`);
  if (result.StandardErrorContent) console.error(`--- stderr ---\n${result.StandardErrorContent}`);

  if (result.Status !== "Success") throw new Error(`El comando terminó con estado ${result.Status}`);
}

// SSM ejecuta como root sin HOME; aws cli y docker compose lo necesitan
const preamble = ["set -eux", "export HOME=/root"];

function deployStack() {
  return runOn(monitoringInstanceName, "Despliega Prometheus, Loki y Grafana desde S3", [
    ...preamble,
    `mkdir -p ${monitoringDir}`,
    // --delete: un archivo borrado en monitoring/ no debe sobrevivir en la EC2
    `aws s3 sync s3://${bucket}/${stack.prefix} ${monitoringDir}/ --delete --region ${region}`,
    `chmod 600 ${monitoringDir}/monitoring.env`,
    `cd ${monitoringDir}`,
    "docker compose -f docker-compose.monitoring.yaml --env-file monitoring.env pull",
    "docker compose -f docker-compose.monitoring.yaml --env-file monitoring.env up -d",
    "docker compose -f docker-compose.monitoring.yaml --env-file monitoring.env ps",
  ]);
}

function deployAlloy(node, lokiUrl) {
  return runOn(node.instanceName, `Despliega Grafana Alloy (${node.nodeName}) desde S3`, [
    ...preamble,
    `mkdir -p ${alloyDir}`,
    `aws s3 sync s3://${bucket}/${alloy.prefix} ${alloyDir}/ --delete --region ${region}`,
    // alloy.env se escribe después del sync (--delete lo borraría) y es propio de cada nodo
    `printf 'NODE_NAME=%s\\nLOKI_URL=%s\\n' '${node.nodeName}' '${lokiUrl}' > ${alloyDir}/alloy.env`,
    `cd ${alloyDir}`,
    "docker compose -f docker-compose.alloy.yaml --env-file alloy.env pull",
    "docker compose -f docker-compose.alloy.yaml --env-file alloy.env up -d",
    "docker compose -f docker-compose.alloy.yaml --env-file alloy.env ps",
  ]);
}

// Borra todo lo que haya bajo monitoring/ y alloy/ (archivos y marcadores de carpeta); no toca el resto del bucket.
async function cleanup() {
  for (const { prefix } of [stack, alloy]) {
    const keys = await listKeys(prefix);
    // DeleteObjects admite hasta 1000 claves por llamada
    for (let i = 0; i < keys.length; i += 1000) {
      const batch = keys.slice(i, i + 1000);
      await s3.send(new DeleteObjectsCommand({ Bucket: bucket, Delete: { Objects: batch.map((Key) => ({ Key })), Quiet: true } }));
    }
    if (keys.length) console.log(`Borrados ${keys.length} objetos de s3://${bucket}/${prefix}`);
  }
}

async function main() {
  const env = checkEnv();
  const lokiUrl = process.env.LOKI_URL ?? `http://${env.MONITORING_PRIVATE_IP}:3100/loki/api/v1/push`;
  try {
    await upload(stack);
    await upload(alloy);
    await deployStack();
    for (const node of alloyNodes) await deployAlloy(node, lokiUrl);
    console.log(`\nListo: monitoreo desplegado. Grafana: https://${env.GRAFANA_DOMAIN} (tras node configure-nginx.js).`);
  } finally {
    // No debe enmascarar el error original del despliegue
    await cleanup().catch((err) => console.error(`AVISO: no se pudo limpiar el bucket (${err.message}); borra monitoring/ y alloy/ a mano.`));
  }
}

main().catch((err) => {
  console.error(err.message ?? err);
  process.exit(1);
});
