import { io } from "socket.io-client";

const url = process.env.SIO_E2E_URL;
if (!url) throw new Error("SIO_E2E_URL is required");

const validAuth = { token: "good-token", workspaceId: "workspace-1" };

async function runClient(name, opts) {
  const socket = io(url, {
    path: "/socket.io/",
    reconnection: false,
    timeout: 3000,
    autoUnref: true,
    auth: validAuth,
    ...opts,
  });

  const gotServerEvent = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${name}: server-event timeout`)), 3000);
    socket.on("server-event", (value) => {
      clearTimeout(timer);
      if (value !== `hello:${name}`) reject(new Error(`${name}: bad server-event ${value}`));
      else resolve();
    });
  });

  await new Promise((resolve, reject) => {
    socket.on("connect", resolve);
    socket.on("connect_error", reject);
  });

  socket.emit("transport", name);
  await gotServerEvent;

  const ack = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${name}: ack timeout`)), 3000);
    socket.emit("client-event", name, (value) => {
      clearTimeout(timer);
      resolve(value);
    });
  });
  if (ack !== `ack:${name}`) throw new Error(`${name}: bad ack ${ack}`);

  const binaryAck = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${name}: binary ack timeout`)), 3000);
    socket.emit("client-binary", new Uint8Array(Buffer.from(`client-bin:${name}`)), (value) => {
      clearTimeout(timer);
      resolve(Buffer.from(value).toString("utf8"));
    });
  });
  if (binaryAck !== `ack-bin:client-bin:${name}`) throw new Error(`${name}: bad binary ack ${binaryAck}`);

  const gotBinaryEvent = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${name}: binary-event timeout`)), 3000);
    socket.on("binary-event", (value) => {
      clearTimeout(timer);
      const text = Buffer.from(value).toString("utf8");
      if (text !== "from-server") reject(new Error(`${name}: bad binary-event ${text}`));
      else resolve();
    });
  });
  socket.emit("server-binary");
  await gotBinaryEvent;

  const disconnectReason = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${name}: disconnect timeout`)), 3000);
    socket.on("disconnect", (reason) => {
      clearTimeout(timer);
      resolve(reason);
    });
  });
  socket.disconnect();
  const reason = await disconnectReason;
  if (reason !== "io client disconnect") throw new Error(`${name}: bad disconnect reason ${reason}`);
  socket.io.engine.close();
}

async function runRejectedClient() {
  const socket = io(url, {
    path: "/socket.io/",
    transports: ["websocket"],
    reconnection: false,
    timeout: 3000,
    autoUnref: true,
    auth: { token: "bad-token", workspaceId: "workspace-1" },
  });
  const message = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("reject: connect_error timeout")), 3000);
    socket.on("connect", () => {
      clearTimeout(timer);
      reject(new Error("reject: unexpectedly connected"));
    });
    socket.on("connect_error", (err) => {
      clearTimeout(timer);
      resolve(err.message);
    });
  });
  if (message !== "unauthorized") throw new Error(`reject: bad connect_error ${message}`);
  socket.disconnect();
  socket.io.engine.close();
}

await runClient("websocket", { transports: ["websocket"] });
await runRejectedClient();
await runClient("polling", {});
