// Despliega Kafka en las EC2 sin SSH: sube docker-compose.yml y .env de ec2/app-1 (ZooKeeper + broker 1
// + Kafka UI) y ec2/app-2 (broker 2) a S3, y vía SSM (SendCommand) cada instancia los descarga del bucket
// (instance profile) y ejecuta `docker compose up -d`. Se ejecuta desde la máquina local tras `terraform apply`.
// También sube las reglas JMX de monitoring/jmx/ y descarga en cada EC2 el JMX Prometheus javaagent
// (/opt/kafka/jmx/), que los compose cargan para exponer métricas a app-monitoring (9404 brokers, 9405 ZooKeeper).
//
// Antes de subir avisa si ya hay copias en el bucket (se sobrescriben). Al terminar, con éxito o error,
// borra del bucket los objetos bajo app-1/ y app-2/ (los .env contienen credenciales: KAFKA_UI_PASSWORD).
//
// Uso:  node deploy-kafka.js   (lee scripts/.env; la shell tiene prioridad sobre el archivo)
// Env:  S3_BUCKET (obligatoria, debe existir) | AWS_REGION | REMOTE_DIR
//       | ZOOKEEPER_INSTANCE_NAME | BROKER_INSTANCE_NAME
//       + credenciales AWS del Learner Lab (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)

import { existsSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { DescribeInstancesCommand, EC2Client } from "@aws-sdk/client-ec2";
import { DeleteObjectsCommand, ListObjectsV2Command, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { GetCommandInvocationCommand, SendCommandCommand, SSMClient, waitUntilCommandExecuted } from "@aws-sdk/client-ssm";

// Relativo al script (no al cwd); loadEnvFile no pisa variables ya definidas en la shell
const envFile = fileURLToPath(new URL(".env", import.meta.url));
if (existsSync(envFile)) process.loadEnvFile(envFile);

const region = process.env.AWS_REGION ?? "us-east-1";
const bucket = process.env.S3_BUCKET;
const remoteDir = process.env.REMOTE_DIR ?? "/opt/kafka";

if (!bucket) {
  console.error("Falta S3_BUCKET (nombre del bucket donde se suben los archivos de ec2/).");
  process.exit(1);
}

// Orden de despliegue: el ZooKeeper (app-1) debe estar arriba antes que el broker 2 (app-2)
const targets = [
  { dir: "app-1", instanceName: process.env.ZOOKEEPER_INSTANCE_NAME ?? "act3-app-zookeper", jmxRules: ["kafka.yml", "zookeeper.yml"] },
  { dir: "app-2", instanceName: process.env.BROKER_INSTANCE_NAME ?? "act3-app-broker-0", jmxRules: ["kafka.yml"] },
];
// Archivos de ec2/<dir>/ + reglas del exporter JMX de monitoring/jmx/ (van a <dir>/jmx/ en S3 y a /opt/kafka/jmx/ en la EC2)
const jmxAgentUrl = "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar";
const filesFor = (target) => [
  { key: `${target.dir}/docker-compose.yml`, path: `../ec2/${target.dir}/docker-compose.yml` },
  { key: `${target.dir}/.env`, path: `../ec2/${target.dir}/.env` },
  ...target.jmxRules.map((rule) => ({ key: `${target.dir}/jmx/${rule}`, path: `../monitoring/jmx/${rule}` })),
];

const ec2 = new EC2Client({ region });
const s3 = new S3Client({ region });
const ssm = new SSMClient({ region });

// Claves existentes bajo app-N/ (incluye el marcador de carpeta "app-N/" si lo hay)
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

async function upload(target) {
  const existing = await listKeys(`${target.dir}/`).catch((err) => {
    if (err.name === "NoSuchBucket") throw new Error(`El bucket ${bucket} no existe en ${region}.`);
    throw err;
  });
  if (existing.length) console.log(`Ya existen en s3://${bucket}/${target.dir}/: ${existing.join(", ")}; se sobrescriben.`);

  for (const { key, path } of filesFor(target)) {
    const localPath = fileURLToPath(new URL(path, import.meta.url));
    if (!existsSync(localPath)) throw new Error(`No existe ${localPath}`);
    try {
      await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: readFileSync(localPath) }));
    } catch (err) {
      if (err.name === "NoSuchBucket") throw new Error(`El bucket ${bucket} no existe en ${region}.`);
      throw err;
    }
    console.log(`Subido s3://${bucket}/${key}`);
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

async function deploy(target) {
  const instanceId = await findInstanceId(target.instanceName);
  console.log(`\n== ${target.instanceName} (${instanceId}) ==`);

  const commands = [
    "set -eux",
    // SSM ejecuta como root sin HOME; aws cli y docker compose lo necesitan
    "export HOME=/root",
    `mkdir -p ${remoteDir}`,
    `aws s3 cp s3://${bucket}/${target.dir}/ ${remoteDir}/ --recursive --region ${region}`,
    `chmod 600 ${remoteDir}/.env`,
    // El jar del agente no va en S3: se baja de Maven Central (salida por NAT/IGW); idempotente
    `[ -s ${remoteDir}/jmx/jmx_prometheus_javaagent.jar ] || curl -fsSL -o ${remoteDir}/jmx/jmx_prometheus_javaagent.jar ${jmxAgentUrl}`,
    `cd ${remoteDir}`,
    "docker compose pull",
    "docker compose up -d",
    "docker compose ps",
  ];

  const { Command } = await ssm.send(
    new SendCommandCommand({
      InstanceIds: [instanceId],
      DocumentName: "AWS-RunShellScript",
      Comment: `Despliega Kafka (${target.dir}) desde S3`,
      Parameters: { commands },
      TimeoutSeconds: 900,
    }),
  );
  const commandId = Command.CommandId;
  console.log(`SSM command ${commandId} enviado; esperando resultado...`);

  // Falla (lanza) si el comando termina en Failed/TimedOut; el detalle se imprime abajo en cualquier caso
  let waitError;
  try {
    await waitUntilCommandExecuted({ client: ssm, maxWaitTime: 900 }, { CommandId: commandId, InstanceId: instanceId });
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

// Borra todo lo que haya bajo app-1/ y app-2/ (archivos y marcadores de carpeta); no toca el resto del bucket.
async function cleanup() {
  for (const target of targets) {
    const keys = await listKeys(`${target.dir}/`);
    if (!keys.length) continue;
    await s3.send(new DeleteObjectsCommand({ Bucket: bucket, Delete: { Objects: keys.map((Key) => ({ Key })), Quiet: true } }));
    console.log(`Borrado de s3://${bucket}/${target.dir}/: ${keys.join(", ")}`);
  }
}

async function main() {
  try {
    for (const target of targets) await upload(target);
    for (const target of targets) await deploy(target);
    console.log("\nListo: Kafka desplegado en ambas instancias.");
  } finally {
    // No debe enmascarar el error original del despliegue
    await cleanup().catch((err) => console.error(`AVISO: no se pudo limpiar el bucket (${err.message}); borra app-1/ y app-2/ a mano.`));
  }
}

main().catch((err) => {
  console.error(err.message ?? err);
  process.exit(1);
});
