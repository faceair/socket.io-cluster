# Kubernetes e2e verification

[中文](./README.CN.md)

This directory contains a reusable Kubernetes verification flow for the built-in Socket.IO cluster mode.

It deploys two real pods with a headless service and verifies that the library works with the default Kubernetes configuration path:

- no `SIO_CLUSTER_SERVICE` when the service name matches the Deployment name;
- no `SIO_CLUSTER_PORT` when `ServerConfig.Port` is set by the server;
- no Kubernetes API watch or RBAC permission;
- peer discovery through headless DNS inferred from `POD_NAME` and `POD_NAMESPACE`.

## Files

- `k8s.yaml` creates the namespace, headless service, and two-pod Deployment.
- `server/main.go` is the e2e server used by the Deployment.
- `client/main.go` is the verifier that connects to both pods through port-forwarding.
- `Dockerfile` packages the server binary into a small test image.
- `run.sh` builds, deploys, waits, port-forwards, and runs the verifier.

Generated files are ignored by git:

- `test/k8s/bin/`
- `test/k8s/.port-forward-*.log`

## Requirements

- `kubectl` connected to a local Kubernetes cluster.
- Docker available to build the local test image.
- The cluster can run images already present in the local Docker runtime, or the script can load the image into `kind` / `minikube` automatically.

The script has been designed for local clusters such as OrbStack, kind, and minikube. If the local cluster cannot pull its own system images, fix that cluster-level issue first; the test image itself is built locally.

## Run

```bash
./test/k8s/run.sh
```

By default it uses:

- namespace: `socketio-cluster-e2e`
- app and service name: `socketio-k8s-e2e`
- image: `socketio-cluster-k8s-e2e:latest`
- port-forward ports: `31081` and `31082`

You can override these values:

```bash
NS=my-sio-e2e APP=my-sio IMAGE=my-sio:e2e PORT_A=32081 PORT_B=32082 ./test/k8s/run.sh
```

## What it verifies

The verifier waits until both pods report at least one cluster peer, then checks:

1. cross-pod broadcast;
2. cross-pod room broadcast;
3. cross-pod broadcast ACK aggregation;
4. cross-node connection state recovery replay.

A successful run ends with:

```text
[k8s-e2e] all k8s socket.io cluster checks passed
```

## Cleanup

`run.sh` recreates the namespace at the beginning of each run. To remove the resources after a run:

```bash
kubectl delete namespace socketio-cluster-e2e --ignore-not-found --wait=true
```
