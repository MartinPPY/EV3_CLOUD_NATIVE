// Configura nginx (reverse proxy) y emite el certificado con certbot en las EC2 públicas, sin parámetros:
//   kafka    app-zookeper    -> Kafka UI  (KAFKA_DOMAIN, upstream 127.0.0.1:KAFKA_UI_PORT)
//   grafana  app-monitoring  -> Grafana   (GRAFANA_DOMAIN, upstream 127.0.0.1:3000)
// Se ejecuta desde la máquina local tras `terraform apply` y los deploy-*.js; usa SSM (SendCommand) porque
// las instancias no exponen SSH. Si una falla, sigue con la otra y termina con error al final.
//
// Uso:  node configure-nginx.js            (configura ambas; lee scripts/.env; la shell tiene prioridad)
//       node configure-nginx.js grafana    (opcional: solo una, kafka o grafana)
// Env:  CERTBOT_EMAIL (obligatoria) | AWS_REGION
//       kafka:   KAFKA_DOMAIN | KAFKA_UI_PORT | INSTANCE_NAME | INSTANCE_ID
//       grafana: GRAFANA_DOMAIN | MONITORING_INSTANCE_NAME
//       + credenciales AWS del Learner Lab (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)

import { existsSync } from "node:fs";
import { resolve4 } from "node:dns/promises";
import { fileURLToPath } from "node:url";
import { DescribeInstancesCommand, EC2Client } from "@aws-sdk/client-ec2";
import { GetCommandInvocationCommand, SendCommandCommand, SSMClient, waitUntilCommandExecuted } from "@aws-sdk/client-ssm";

// Relativo al script (no al cwd); loadEnvFile no pisa variables ya definidas en la shell
const envFile = fileURLToPath(new URL(".env", import.meta.url));
if (existsSync(envFile)) process.loadEnvFile(envFile);

const region = process.env.AWS_REGION ?? "us-east-1";
const email = process.env.CERTBOT_EMAIL;

const targets = {
  kafka: {
    label: "Kafka UI",
    vhost: "kafka-ui",
    domain: process.env.KAFKA_DOMAIN ?? "kafka.ejemplo.com",
    upstreamPort: process.env.KAFKA_UI_PORT ?? "8080",
    instanceName: process.env.INSTANCE_NAME ?? "act3-app-zookeper",
    instanceId: process.env.INSTANCE_ID,
    extraLocations: "",
  },
  grafana: {
    label: "Grafana",
    vhost: "grafana",
    domain: process.env.GRAFANA_DOMAIN ?? "grafana.ejemplo.com",
    upstreamPort: "3000",
    instanceName: process.env.MONITORING_INSTANCE_NAME ?? "act3-app-monitoring",
    instanceId: undefined,
    // Grafana Live (paneles en tiempo real) usa WebSocket
    extraLocations: `
    location /api/live/ {
        proxy_pass http://127.0.0.1:3000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
    }
`,
  },
};

const requested = process.argv[2];
if (requested && !targets[requested]) {
  console.error(`Destino desconocido: ${requested} (usa kafka o grafana, o ninguno para ambos).`);
  process.exit(1);
}
const selected = requested ? [requested] : Object.keys(targets);

if (!email) {
  console.error("Falta CERTBOT_EMAIL (correo para avisos de Let's Encrypt).");
  process.exit(1);
}

const ec2 = new EC2Client({ region });
const ssm = new SSMClient({ region });

// El ID de la instancia cambia en cada despliegue: se busca por tag Name salvo que se indique.
async function findInstance(instanceName) {
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
  return instance;
}

function nginxConfFor({ domain, upstreamPort, extraLocations }) {
  return `server {
    listen 80;
    listen [::]:80;
    server_name ${domain};

    location / {
        proxy_pass http://127.0.0.1:${upstreamPort};
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        # Kafka UI usa Server-Sent Events (live tail de mensajes): sin buffering ni timeout corto (inocuo en Grafana)
        proxy_buffering off;
        proxy_read_timeout 3600s;
    }
${extraLocations}}
`;
}

function commandsFor(target) {
  // La config va en base64 para no lidiar con el escapado de $host, comillas, etc. en el shell remoto.
  const confB64 = Buffer.from(nginxConfFor(target)).toString("base64");
  const { vhost, domain } = target;
  return [
    "set -eux",
    `echo '${confB64}' | base64 -d > /etc/nginx/sites-available/${vhost}`,
    `ln -sf /etc/nginx/sites-available/${vhost} /etc/nginx/sites-enabled/${vhost}`,
    "rm -f /etc/nginx/sites-enabled/default",
    "nginx -t",
    "systemctl reload nginx",
    // --redirect añade el 301 a HTTPS; rerun seguro: certbot reutiliza el certificado vigente
    `certbot --nginx -d ${domain} --non-interactive --agree-tos -m ${email} --redirect`,
    "nginx -t && systemctl reload nginx",
  ];
}

async function warnIfDnsMismatch(domain, publicIp) {
  try {
    const ips = await resolve4(domain);
    if (!ips.includes(publicIp)) {
      console.warn(`AVISO: ${domain} resuelve a ${ips.join(", ")} pero la instancia tiene ${publicIp}; certbot fallará hasta que el DNS apunte a esa IP pública.`);
    }
  } catch (err) {
    console.warn(`AVISO: ${domain} no resuelve (${err.code ?? err.message}); certbot fallará hasta que exista el registro A.`);
  }
}

async function configure(target) {
  console.log(`\n== ${target.label} (${target.domain}) ==`);
  let instanceId = target.instanceId;
  if (!instanceId) {
    const instance = await findInstance(target.instanceName);
    instanceId = instance.InstanceId;
    console.log(`Instancia ${target.instanceName}: ${instanceId} (IP pública ${instance.PublicIpAddress ?? "ninguna"})`);
    if (instance.PublicIpAddress) await warnIfDnsMismatch(target.domain, instance.PublicIpAddress);
  }

  const { Command } = await ssm.send(
    new SendCommandCommand({
      InstanceIds: [instanceId],
      DocumentName: "AWS-RunShellScript",
      Comment: `Configura nginx + certbot para ${target.label}`,
      Parameters: { commands: commandsFor(target) },
      TimeoutSeconds: 600,
    }),
  );
  const commandId = Command.CommandId;
  console.log(`SSM command ${commandId} enviado; esperando resultado...`);

  // El waiter lanza (con el JSON completo de la invocación) si el comando termina en Failed/TimedOut;
  // se ignora y se informa abajo con stdout/stderr y el estado, que es lo útil.
  await waitUntilCommandExecuted({ client: ssm, maxWaitTime: 600 }, { CommandId: commandId, InstanceId: instanceId }).catch(() => {});

  const result = await ssm.send(new GetCommandInvocationCommand({ CommandId: commandId, InstanceId: instanceId }));
  console.log(`Estado: ${result.Status}`);
  if (result.StandardOutputContent) console.log(`--- stdout ---\n${result.StandardOutputContent}`);
  if (result.StandardErrorContent) console.error(`--- stderr ---\n${result.StandardErrorContent}`);

  if (result.Status !== "Success") throw new Error(`El comando terminó con estado ${result.Status}`);
  console.log(`Listo: https://${target.domain}`);
}

async function main() {
  const failed = [];
  for (const name of selected) {
    try {
      await configure(targets[name]);
    } catch (err) {
      console.error(`ERROR en ${name}: ${err.message ?? err}`);
      failed.push(name);
    }
  }
  if (failed.length) {
    console.error(`\nFallaron: ${failed.join(", ")}`);
    process.exit(1);
  }
}

main();
