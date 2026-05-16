import { io } from "socket.io-client";

const url = process.env.SIO_E2E_URL;
if (!url) throw new Error("SIO_E2E_URL is required");

const validAuth = { token: "good-token", workspaceId: "workspace-1" };

function connectSocket(name, opts) {
  return io(url, {
    path: "/socket.io/",
    reconnection: false,
    timeout: 3000,
    autoUnref: true,
    forceNew: true,
    auth: validAuth,
    ...opts,
  });
}

function waitForConnect(socket, name) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${name}: connect timeout`)), 3000);
    socket.on("connect", () => {
      clearTimeout(timer);
      resolve();
    });
    socket.on("connect_error", (err) => {
      clearTimeout(timer);
      reject(err);
    });
  });
}

async function runClient(name, opts) {
  const socket = connectSocket(name, opts);

  const gotServerEvent = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${name}: server-event timeout`)), 3000);
    socket.on("server-event", (value) => {
      clearTimeout(timer);
      if (value !== `hello:${name}`) reject(new Error(`${name}: bad server-event ${value}`));
      else resolve();
    });
  });

  await waitForConnect(socket, name);

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
  const socket = connectSocket("reject", {
    transports: ["websocket"],
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

async function runNamespaceClient() {
  const socket = io(`${url}/workspace`, {
    path: "/socket.io/",
    transports: ["websocket"],
    reconnection: false,
    timeout: 3000,
    autoUnref: true,
    forceNew: true,
    auth: { token: "workspace-token", workspaceId: "workspace-1" },
  });
  await waitForConnect(socket, "namespace");
  const ack = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("namespace: ack timeout")), 3000);
    socket.emit("namespace-event", "workspace", (value) => {
      clearTimeout(timer);
      resolve(value);
    });
  });
  if (ack !== "namespace-ack:workspace") throw new Error(`namespace: bad ack ${ack}`);
  socket.disconnect();
  socket.io.engine.close();
}

async function runRejectedNamespaceClient() {
  const socket = io(`${url}/workspace`, {
    path: "/socket.io/",
    transports: ["websocket"],
    reconnection: false,
    timeout: 3000,
    autoUnref: true,
    forceNew: true,
    auth: { token: "bad-token", workspaceId: "workspace-1" },
  });
  const message = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("namespace reject: connect_error timeout")), 3000);
    socket.on("connect", () => {
      clearTimeout(timer);
      reject(new Error("namespace reject: unexpectedly connected"));
    });
    socket.on("connect_error", (err) => {
      clearTimeout(timer);
      resolve(err.message);
    });
  });
  if (message !== "workspace unauthorized") throw new Error(`namespace reject: bad connect_error ${message}`);
  socket.disconnect();
  socket.io.engine.close();
}

async function runDynamicNamespaceClient() {
  const socket = io(`${url}/dynamic`, {
    path: "/socket.io/",
    transports: ["websocket"],
    reconnection: false,
    timeout: 3000,
    autoUnref: true,
    forceNew: true,
    auth: { token: "dynamic-token", workspaceId: "workspace-1" },
  });
  await waitForConnect(socket, "dynamic namespace");
  socket.disconnect();
  socket.io.engine.close();
}

async function runReconnectClient() {
  const socket = io(url, {
    path: "/socket.io/",
    transports: ["websocket"],
    reconnection: true,
    reconnectionAttempts: 3,
    reconnectionDelay: 50,
    reconnectionDelayMax: 50,
    timeout: 3000,
    autoUnref: true,
    forceNew: true,
    auth: { token: "reconnect-token", workspaceId: "workspace-reconnect" },
  });

  let connects = 0;
  let sawTransportDisconnect = false;
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`reconnect: timeout connects=${connects}`)), 5000);
    socket.on("connect", () => {
      connects++;
      if (connects === 1) {
        socket.emit("force-transport-close");
        return;
      }
      socket.emit("auth-count", "reconnect-token", (count) => {
        clearTimeout(timer);
        if (!sawTransportDisconnect) reject(new Error("reconnect: missing transport close disconnect"));
        else if (count < 2) reject(new Error(`reconnect: auth count ${count}`));
        else resolve();
      });
    });
    socket.on("disconnect", (reason) => {
      if (reason === "transport close") sawTransportDisconnect = true;
    });
    socket.on("connect_error", reject);
    socket.io.on("reconnect_failed", () => reject(new Error("reconnect: reconnect_failed")));
  });
  socket.disconnect();
  socket.io.engine.close();
}

await runClient("websocket", { transports: ["websocket"] });
await runRejectedClient();
await runNamespaceClient();
await runRejectedNamespaceClient();
await runDynamicNamespaceClient();
await runReconnectClient();
if (process.env.SIO_JS_E2E_POLLING === "1") {
  await runClient("polling", {});
}
