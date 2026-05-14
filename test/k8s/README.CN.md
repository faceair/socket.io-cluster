# Kubernetes e2e 验证

[English](./README.md)

这个目录沉淀了内置 Socket.IO cluster 模式的可复用 Kubernetes 验证流程。

它会部署两个真实 Pod 和一个 headless service，并验证默认 Kubernetes 配置路径可工作：

- 当 service 名和 Deployment 名一致时，不需要 `SIO_CLUSTER_SERVICE`；
- server 已通过 `ServerConfig.Port` 设置端口时，不需要 `SIO_CLUSTER_PORT`；
- 不需要 Kubernetes API watch 或 RBAC 权限；
- 通过 `POD_NAME` 和 `POD_NAMESPACE` 推断出的 headless DNS 自动发现 peers。

## 文件

- `k8s.yaml` 创建 namespace、headless service 和双 Pod Deployment。
- `server/main.go` 是 Deployment 使用的 e2e server。
- `client/main.go` 是通过 port-forward 同时连接两个 Pod 的验证器。
- `Dockerfile` 把 server 二进制打进轻量测试镜像。
- `run.sh` 负责编译、构建镜像、部署、等待、端口转发和执行验证。

以下生成物已加入 git ignore：

- `test/k8s/bin/`
- `test/k8s/.port-forward-*.log`

## 依赖

- `kubectl` 已连接到本地 Kubernetes 集群。
- Docker 可用于构建本地测试镜像。
- 集群能使用本地 Docker runtime 中已有的镜像，或者脚本能自动把镜像加载到 `kind` / `minikube`。

脚本面向 OrbStack、kind、minikube 这类本地集群。如果本地集群自身的系统镜像都无法拉取，需要先修好该集群问题；测试应用镜像本身会在本地构建。

## 运行

```bash
./test/k8s/run.sh
```

默认值：

- namespace：`socketio-cluster-e2e`
- app 和 service 名：`socketio-k8s-e2e`
- image：`socketio-cluster-k8s-e2e:latest`
- port-forward 端口：`31081` 和 `31082`

可以通过环境变量覆盖：

```bash
NS=my-sio-e2e APP=my-sio IMAGE=my-sio:e2e PORT_A=32081 PORT_B=32082 ./test/k8s/run.sh
```

## 验证内容

验证器会等待两个 Pod 都至少发现一个 cluster peer，然后检查：

1. 跨 Pod broadcast；
2. 跨 Pod room broadcast；
3. 跨 Pod broadcast ACK 聚合；
4. 跨节点 connection state recovery replay。

成功运行会以这行输出结束：

```text
[k8s-e2e] all k8s socket.io cluster checks passed
```

## 清理

`run.sh` 每次开始都会重建 namespace。运行结束后如需删除资源：

```bash
kubectl delete namespace socketio-cluster-e2e --ignore-not-found --wait=true
```
