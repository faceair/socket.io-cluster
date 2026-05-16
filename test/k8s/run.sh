#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
NS=${NS:-socketio-cluster-e2e}
APP=${APP:-socketio-k8s-e2e}
IMAGE=${IMAGE:-socketio-cluster-k8s-e2e:latest}
PORT_A=${PORT_A:-31081}
PORT_B=${PORT_B:-31082}
manifest=""
pf_a=""
pf_b=""

cd "$ROOT"

cleanup() {
  if [[ -n "${pf_a:-}" ]]; then kill "$pf_a" 2>/dev/null || true; fi
  if [[ -n "${pf_b:-}" ]]; then kill "$pf_b" 2>/dev/null || true; fi
  if [[ "${KEEP_K8S_E2E:-0}" != "1" ]]; then
    kubectl delete namespace "$NS" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  fi
  if [[ -n "${manifest:-}" ]]; then rm -f "$manifest"; fi
  wait 2>/dev/null || true
}
trap cleanup EXIT

sed_escape() {
  printf '%s' "$1" | sed -e 's/[\/&]/\\&/g'
}

arch=$(kubectl get nodes -o jsonpath='{.items[0].status.nodeInfo.architecture}')
case "$arch" in
  amd64|arm64) ;;
  *) echo "unsupported Kubernetes node architecture: $arch" >&2; exit 1 ;;
esac

mkdir -p test/k8s/bin
GOOS=linux GOARCH="$arch" CGO_ENABLED=0 go build -o test/k8s/bin/socketio-k8s-server ./test/k8s/server
docker build -f test/k8s/Dockerfile -t "$IMAGE" test/k8s >/dev/null

if command -v kind >/dev/null 2>&1 && kubectl config current-context | grep -q '^kind-'; then
  kind load docker-image "$IMAGE"
elif command -v minikube >/dev/null 2>&1 && kubectl config current-context | grep -q '^minikube$'; then
  minikube image load "$IMAGE"
fi

manifest=$(mktemp)
sed \
  -e "s/socketio-cluster-e2e/$(sed_escape "$NS")/g" \
  -e "s/socketio-k8s-e2e/$(sed_escape "$APP")/g" \
  -e "s/socketio-cluster-k8s-e2e:latest/$(sed_escape "$IMAGE")/g" \
  test/k8s/k8s.yaml >"$manifest"

kubectl delete namespace "$NS" --ignore-not-found --wait=true
kubectl apply -f "$manifest"
kubectl -n "$NS" rollout status deploy/"$APP" --timeout=120s

for _ in {1..120}; do
  ready_count=$(kubectl -n "$NS" get pods -l app="$APP" -o jsonpath='{range .items[*]}{range .status.conditions[?(@.type=="Ready")]}{.status}{"\n"}{end}{end}' | grep -c '^True$' || true)
  if [[ "$ready_count" -ge 2 ]]; then
    break
  fi
  sleep 1
done
if [[ "${ready_count:-0}" -lt 2 ]]; then
  kubectl -n "$NS" get pods -o wide >&2
  exit 1
fi

pods=()
while IFS= read -r pod; do
  [[ -n "$pod" ]] && pods+=("$pod")
done < <(kubectl -n "$NS" get pods -l app="$APP" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sort)
if [[ ${#pods[@]} -lt 2 ]]; then
  echo "expected at least 2 pods, got ${#pods[@]}" >&2
  exit 1
fi

kubectl -n "$NS" port-forward "pod/${pods[0]}" "${PORT_A}:3000" >test/k8s/.port-forward-a.log 2>&1 & pf_a=$!
kubectl -n "$NS" port-forward "pod/${pods[1]}" "${PORT_B}:3000" >test/k8s/.port-forward-b.log 2>&1 & pf_b=$!

for _ in {1..50}; do
  if grep -q 'Forwarding from' test/k8s/.port-forward-a.log 2>/dev/null && grep -q 'Forwarding from' test/k8s/.port-forward-b.log 2>/dev/null; then
    break
  fi
  sleep 0.1
done

if ! kill -0 "$pf_a" 2>/dev/null || ! kill -0 "$pf_b" 2>/dev/null; then
  cat test/k8s/.port-forward-a.log test/k8s/.port-forward-b.log >&2 || true
  exit 1
fi

go run ./test/k8s/client --pod-a "http://127.0.0.1:${PORT_A}" --pod-b "http://127.0.0.1:${PORT_B}"

kubectl -n "$NS" get pods -o wide
