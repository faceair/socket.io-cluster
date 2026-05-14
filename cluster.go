package sio

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type clusterNode struct {
	server         *Server
	nodeID         string
	advertiseURL   string
	path           string
	mu             sync.RWMutex
	peers          []string
	dnsNames       []string
	client         *http.Client
	requestTimeout time.Duration
	jobs           chan clusterJob
	workerCount    int
}

type socketSnapshot struct {
	ID    SocketID `json:"id"`
	Rooms []Room   `json:"rooms"`
}

type clusterJob struct {
	op       string
	peer     string
	endpoint string
	body     []byte
	reply    chan<- clusterResponse
}

type clusterPacketBody struct {
	owner       *pooledBytes
	attachments *pooledByteViews
	packet      []byte
}

func newClusterNode(server *Server, port string, config ClusterConfig) (*clusterNode, error) {
	path := server.path
	nodeID := config.NodeID
	if nodeID == "" {
		nodeID = server.ids.node
	}
	advertiseURL, err := defaultAdvertiseURL(port, config)
	if err != nil {
		return nil, err
	}
	c := &clusterNode{
		server:         server,
		nodeID:         nodeID,
		advertiseURL:   strings.TrimRight(advertiseURL, "/"),
		path:           path,
		peers:          normalizePeers(defaultPeers(config.Peers), path),
		dnsNames:       defaultHeadlessDNS(config.HeadlessDNS),
		client:         &http.Client{Timeout: cmp.Or(config.RequestTimeout, 2*time.Second)},
		requestTimeout: cmp.Or(config.RequestTimeout, 2*time.Second),
		workerCount:    cmp.Or(config.FanoutWorkers, 8),
	}
	if c.workerCount < 1 {
		c.workerCount = 1
	}
	c.refreshDNSPeers()
	if len(c.peers) != 0 || len(c.dnsNames) != 0 {
		c.jobs = make(chan clusterJob, c.workerCount*4)
		for i := 0; i < c.workerCount; i++ {
			server.lc.start(fmt.Sprintf("cluster-fanout-%d", i), c.runFanoutWorker)
		}
	}
	if len(c.dnsNames) != 0 {
		interval := cmp.Or(config.HeartbeatInterval, 30*time.Second)
		server.lc.start("cluster-dns-refresh", func(ctx context.Context) {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					c.refreshDNSPeers()
				}
			}
		})
	}
	return c, nil
}

func resolveNodeID(configured string) string {
	if configured != "" {
		return configured
	}
	for _, key := range [...]string{"POD_NAME", "HOSTNAME"} {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
	}
	if hostname, err := os.Hostname(); err == nil && hostname != "" {
		return hostname
	}
	return ""
}

func defaultPeers(configured []string) []string {
	if len(configured) != 0 {
		return configured
	}
	if peers := splitEnvList("SIO_CLUSTER_PEERS"); len(peers) != 0 {
		return peers
	}
	return splitEnvList("SOCKETIO_CLUSTER_PEERS")
}

func defaultHeadlessDNS(configured []string) []string {
	if len(configured) != 0 {
		return configured
	}
	if names := splitEnvList("SIO_CLUSTER_HEADLESS_DNS"); len(names) != 0 {
		return names
	}
	if names := splitEnvList("SOCKETIO_CLUSTER_HEADLESS_DNS"); len(names) != 0 {
		return names
	}
	service := defaultClusterServiceName()
	if service == "" {
		return nil
	}
	namespace := defaultKubernetesNamespace()
	if namespace == "" {
		return []string{service}
	}
	return []string{service + "." + namespace + ".svc"}
}

func defaultAdvertiseURL(port string, config ClusterConfig) (string, error) {
	if value := firstNonEmpty(
		config.AdvertiseURL,
		os.Getenv("SIO_CLUSTER_ADVERTISE_URL"),
		os.Getenv("SOCKETIO_CLUSTER_ADVERTISE_URL"),
		os.Getenv("SOCKETIO_ADVERTISE_URL"),
	); value != "" {
		return validateAdvertiseURL(value)
	}
	port, ok := defaultClusterPort(port)
	if !ok {
		return "", fmt.Errorf("sio cluster: ServerConfig.Port is required unless Cluster.AdvertiseURL is set; set ServerConfig.Port or SIO_CLUSTER_PORT/PORT/<SERVICE>_SERVICE_PORT")
	}
	if err := validatePort(port); err != nil {
		return "", err
	}
	if port == "" {
		return "", fmt.Errorf("sio cluster: resolved empty ServerConfig.Port")
	}
	host := defaultAdvertiseHost()
	if host == "" {
		return "", fmt.Errorf("sio cluster: cannot resolve advertise host")
	}
	scheme := firstNonEmpty(os.Getenv("SIO_CLUSTER_SCHEME"), os.Getenv("SOCKETIO_CLUSTER_SCHEME"), "http")
	return scheme + "://" + net.JoinHostPort(host, port), nil
}

func validateAdvertiseURL(value string) (string, error) {
	u, err := url.Parse(value)
	if err != nil {
		return "", fmt.Errorf("sio cluster: parse AdvertiseURL %q failed: %w", value, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("sio cluster: AdvertiseURL %q must use http or https scheme", value)
	}
	if u.Hostname() == "" {
		return "", fmt.Errorf("sio cluster: AdvertiseURL %q must include host", value)
	}
	if u.Port() == "" {
		return "", fmt.Errorf("sio cluster: AdvertiseURL %q must include port", value)
	}
	if err := validatePort(u.Port()); err != nil {
		return "", fmt.Errorf("sio cluster: AdvertiseURL %q has invalid port: %w", value, err)
	}
	return value, nil
}

func validatePort(port string) error {
	n, err := strconv.Atoi(port)
	if err != nil || n <= 0 || n > 65535 {
		return fmt.Errorf("sio cluster: invalid ServerConfig.Port %q", port)
	}
	return nil
}

func defaultAdvertiseHost() string {
	if host := firstNonEmpty(
		os.Getenv("SIO_CLUSTER_HOST"),
		os.Getenv("SOCKETIO_CLUSTER_HOST"),
		os.Getenv("POD_IP"),
		os.Getenv("HOST_IP"),
	); host != "" {
		return host
	}
	if host := defaultPodDNSHost(); host != "" {
		return host
	}
	if host := firstNonLoopbackIP(); host != "" {
		return host
	}
	if hostname, err := os.Hostname(); err == nil {
		if hostname = strings.TrimSpace(hostname); hostname != "" {
			return hostname
		}
	}
	return "127.0.0.1"
}

func defaultPodDNSHost() string {
	pod := firstNonEmpty(os.Getenv("POD_NAME"), os.Getenv("HOSTNAME"))
	service := defaultClusterServiceName()
	if pod == "" || service == "" {
		return ""
	}
	namespace := defaultKubernetesNamespace()
	if namespace == "" {
		return pod + "." + service
	}
	return pod + "." + service + "." + namespace + ".svc"
}

func defaultClusterServiceName() string {
	if service := firstNonEmpty(os.Getenv("SIO_CLUSTER_SERVICE"), os.Getenv("SOCKETIO_CLUSTER_SERVICE"), os.Getenv("SERVICE_NAME")); service != "" {
		return service
	}
	return inferClusterServiceName(firstNonEmpty(os.Getenv("POD_NAME"), os.Getenv("HOSTNAME")))
}

func defaultKubernetesNamespace() string {
	return firstNonEmpty(os.Getenv("POD_NAMESPACE"), os.Getenv("NAMESPACE"), os.Getenv("KUBERNETES_NAMESPACE"))
}

func inferClusterServiceName(podName string) string {
	parts := strings.Split(strings.TrimSpace(podName), "-")
	if len(parts) < 2 {
		return ""
	}
	last := parts[len(parts)-1]
	if allDigits(last) {
		return strings.Join(parts[:len(parts)-1], "-")
	}
	if len(parts) >= 3 && isKubernetesHash(parts[len(parts)-2], 8, 12) && isKubernetesHash(last, 5, 5) {
		return strings.Join(parts[:len(parts)-2], "-")
	}
	return ""
}

func allDigits(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

func isKubernetesHash(value string, minLen, maxLen int) bool {
	if len(value) < minLen || len(value) > maxLen {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'z') {
			return false
		}
	}
	return true
}

func defaultClusterPort(configured string) (string, bool) {
	if port := firstNonEmpty(
		configured,
		os.Getenv("SIO_CLUSTER_PORT"),
		os.Getenv("SOCKETIO_CLUSTER_PORT"),
		os.Getenv("SOCKETIO_PORT"),
		os.Getenv("PORT"),
		os.Getenv("HTTP_PORT"),
	); port != "" {
		return strings.TrimPrefix(port, ":"), true
	}
	if service := defaultClusterServiceName(); service != "" {
		if port := strings.TrimPrefix(os.Getenv(serviceEnvKey(service)+"_SERVICE_PORT"), ":"); port != "" {
			return port, true
		}
	}
	return "", false
}

func serviceEnvKey(service string) string {
	var b strings.Builder
	b.Grow(len(service))
	for i := 0; i < len(service); i++ {
		c := service[i]
		switch {
		case c >= 'a' && c <= 'z':
			b.WriteByte(c - 'a' + 'A')
		case c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
			b.WriteByte(c)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

func firstNonLoopbackIP() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return ""
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() || ip.IsUnspecified() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
				continue
			}
			if v4 := ip.To4(); v4 != nil {
				return v4.String()
			}
			return ip.String()
		}
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func splitEnvList(key string) []string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func normalizePeers(peers []string, path string) []string {
	out := make([]string, 0, len(peers))
	for _, peer := range peers {
		peer = strings.TrimSpace(peer)
		if peer == "" {
			continue
		}
		if !strings.HasPrefix(peer, "http://") && !strings.HasPrefix(peer, "https://") {
			peer = "http://" + peer
		}
		if strings.Contains(peer, "?transport=cluster") {
			out = append(out, peer)
			continue
		}
		if !strings.HasSuffix(peer, path) {
			peer = strings.TrimRight(peer, "/") + path + "?transport=cluster"
		}
		out = append(out, peer)
	}
	return out
}

func (c *clusterNode) peerSnapshot() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, len(c.peers))
	copy(out, c.peers)
	return out
}

func (c *clusterNode) refreshDNSPeers() {
	if len(c.dnsNames) == 0 || c.advertiseURL == "" {
		return
	}
	u, err := url.Parse(c.advertiseURL)
	if err != nil {
		return
	}
	_, port, _ := net.SplitHostPort(u.Host)
	if port == "" {
		return
	}
	selfHosts := map[string]struct{}{}
	if host := strings.TrimSpace(u.Hostname()); host != "" {
		selfHosts[host] = struct{}{}
	}
	if podIP := strings.TrimSpace(os.Getenv("POD_IP")); podIP != "" {
		selfHosts[podIP] = struct{}{}
	}
	peers := make([]string, 0, len(c.peers))
	for _, name := range c.dnsNames {
		ips, err := net.LookupHost(name)
		if err != nil {
			continue
		}
		for _, ip := range ips {
			if _, self := selfHosts[ip]; self {
				continue
			}
			peer := u.Scheme + "://" + net.JoinHostPort(ip, port) + c.path + "?transport=cluster"
			if peer != c.advertiseURL+c.path+"?transport=cluster" {
				peers = append(peers, peer)
			}
		}
	}
	c.mu.Lock()
	c.peers = peers
	c.mu.Unlock()
}

func (c *clusterNode) close() {}

func (c *clusterNode) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	c.server.metrics.clusterRequestsReceived.Add(1)
	if r.Header.Get("X-Sio-Origin") == c.nodeID {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	q := r.URL.Query()
	op := q.Get("op")
	nspName := q.Get("nsp")
	if nspName == "" {
		nspName = "/"
	}
	nsp, ok := c.server.getNamespace(nspName)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	opts := broadcastOptions{Rooms: valuesToRooms(q["room"]), Except: valuesToRooms(q["except"]), Flags: broadcastFlags{Local: true}}
	switch op {
	case "broadcast":
		body, err := readClusterPacket(r)
		if err != nil {
			writeEngineError(w, 3, "Bad request")
			return
		}
		defer body.Release()
		ackID, _ := strconv.ParseUint(q.Get("ack"), 10, 64)
		origin := q.Get("origin")
		c.logRemoteRecoveryPacket(nsp.name, opts, body.packet, body.Attachments())
		count := nsp.adapter.apply(opts, func(s *serverSocket) {
			if ackID != 0 && origin != "" {
				s.conn.registerBroadcastAck(ackID, &remoteAckForwarder{client: c.client, origin: origin, id: ackID})
			}
			s.conn.sendSocketPayload(body.packet, body.Attachments())
		})
		w.Header().Set("X-Sio-Expected", strconv.Itoa(count))
		w.WriteHeader(http.StatusNoContent)
	case "ack":
		id, _ := strconv.ParseUint(q.Get("id"), 10, 64)
		body, err := readAllPooled(r.Body, r.ContentLength)
		if err != nil {
			writeEngineError(w, 3, "Bad request")
			return
		}
		defer body.Release()
		args, err := NewJSONArrayView(body.B)
		if err != nil {
			writeEngineError(w, 3, "Bad request")
			return
		}
		if tracker, ok := c.server.getBroadcastAck(id); ok {
			tracker.accept(args, nil)
		}
		w.WriteHeader(http.StatusNoContent)
	case "join":
		rooms := valuesToRooms(q["target"])
		nsp.adapter.apply(opts, func(s *serverSocket) { s.Join(rooms...) })
		w.WriteHeader(http.StatusNoContent)
	case "leave":
		rooms := valuesToRooms(q["target"])
		nsp.adapter.apply(opts, func(s *serverSocket) {
			for _, room := range rooms {
				s.Leave(room)
			}
		})
		w.WriteHeader(http.StatusNoContent)
	case "disconnect":
		closeConn := q.Get("close") == "1"
		nsp.adapter.apply(opts, func(s *serverSocket) { s.Disconnect(closeConn) })
		w.WriteHeader(http.StatusNoContent)
	case "fetch":
		local := c.fetchLocal(nsp, opts)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(local)
	case "csr":
		if c.server.recovery == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		pid := q.Get("pid")
		session, replay, ok := c.server.recovery.snapshot(nsp.name, pid, q.Get("offset"), time.Now())
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		defer session.release()
		defer releaseReplayPackets(replay)
		body := encodeCSRResponse(session, replay)
		defer body.Release()
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write(body.B); err == nil {
			c.server.recovery.deleteSession(nsp.name, pid)
		}
	case "sse":
		body, err := readClusterPacket(r)
		if err != nil {
			writeEngineError(w, 3, "Bad request")
			return
		}
		defer body.Release()
		p, err := ParsePacketView(body.packet)
		if err != nil {
			writeEngineError(w, 3, "Bad request")
			return
		}
		p.Binary = body.Attachments()
		if err := nsp.handlers.dispatchServerSide(p); err != nil {
			c.server.reportError(err)
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeEngineError(w, 3, "Bad request")
	}
}

func (c *clusterNode) logRemoteRecoveryPacket(namespace string, opts broadcastOptions, packet []byte, attachments [][]byte) {
	if c.server.recovery == nil {
		return
	}
	offset := packetRecoveryOffset(packet)
	if offset == "" {
		return
	}
	c.server.recovery.log(namespace, opts, offset, packet, attachments, time.Now())
}

func packetRecoveryOffset(packet []byte) string {
	p, err := ParsePacketView(packet)
	if err != nil || (p.Type != PacketEvent && p.Type != PacketBinaryEvent) || p.HasID {
		return ""
	}
	args := p.Args
	var last []byte
	for {
		raw, ok, err := args.Next()
		if err != nil {
			return ""
		}
		if !ok {
			break
		}
		last = raw
	}
	if len(last) == 0 {
		return ""
	}
	offset, err := unquoteJSONStringView(last)
	if err != nil {
		return ""
	}
	return bytesToStringView(offset)
}

var clusterBinaryMagic = [...]byte{'S', 'I', 'O', 'B'}
var clusterCSRMagic = [...]byte{'S', 'I', 'O', 'C', 'S', 'R'}

func encodeClusterPacket(packet []byte, attachments [][]byte) ([]byte, *pooledBytes) {
	if len(attachments) == 0 {
		return packet, nil
	}
	total := len(clusterBinaryMagic) + binary.MaxVarintLen64*2 + len(packet)
	for _, attachment := range attachments {
		total += binary.MaxVarintLen64 + len(attachment)
	}
	owner := acquireBytes(total)
	owner.AppendBytes(clusterBinaryMagic[:])
	appendUvarint(owner, uint64(len(packet)))
	owner.AppendBytes(packet)
	appendUvarint(owner, uint64(len(attachments)))
	for _, attachment := range attachments {
		appendUvarint(owner, uint64(len(attachment)))
		owner.AppendBytes(attachment)
	}
	return owner.B, owner
}

func readClusterPacket(r *http.Request) (*clusterPacketBody, error) {
	owner, err := readAllPooled(r.Body, r.ContentLength)
	if err != nil {
		return nil, err
	}
	if len(owner.B) < len(clusterBinaryMagic) || !bytes.Equal(owner.B[:len(clusterBinaryMagic)], clusterBinaryMagic[:]) {
		return &clusterPacketBody{owner: owner, packet: owner.B}, nil
	}
	pos := len(clusterBinaryMagic)
	packetLen, n := binary.Uvarint(owner.B[pos:])
	if n <= 0 {
		owner.Release()
		return nil, fmt.Errorf("sio cluster: invalid binary packet length")
	}
	pos += n
	if packetLen > uint64(len(owner.B)-pos) {
		owner.Release()
		return nil, fmt.Errorf("sio cluster: binary packet length exceeds body")
	}
	packet := owner.B[pos : pos+int(packetLen)]
	pos += int(packetLen)
	count, n := binary.Uvarint(owner.B[pos:])
	if n <= 0 {
		owner.Release()
		return nil, fmt.Errorf("sio cluster: invalid attachment count")
	}
	pos += n
	var views *pooledByteViews
	if count != 0 {
		views = acquireByteViews(int(count))
	}
	for i := 0; i < int(count); i++ {
		size, n := binary.Uvarint(owner.B[pos:])
		if n <= 0 {
			if views != nil {
				views.Release()
			}
			owner.Release()
			return nil, fmt.Errorf("sio cluster: invalid attachment length")
		}
		pos += n
		if size > uint64(len(owner.B)-pos) {
			if views != nil {
				views.Release()
			}
			owner.Release()
			return nil, fmt.Errorf("sio cluster: attachment length exceeds body")
		}
		views.Append(owner.B[pos : pos+int(size)])
		pos += int(size)
	}
	return &clusterPacketBody{owner: owner, packet: packet, attachments: views}, nil
}

func appendUvarint(dst *pooledBytes, v uint64) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	dst.AppendBytes(buf[:n])
}

func encodeCSRResponse(session *recoverySession, replay []recoveryPacket) *pooledBytes {
	total := len(clusterCSRMagic) + len(session.sid) + len(session.pid) + binary.MaxVarintLen64*4
	for _, room := range session.rooms {
		total += len(room) + binary.MaxVarintLen64
	}
	for _, packet := range replay {
		total += len(packet.packet) + binary.MaxVarintLen64*2
		for _, attachment := range packet.attachmentViews() {
			total += len(attachment) + binary.MaxVarintLen64
		}
	}
	out := acquireBytes(total)
	out.AppendBytes(clusterCSRMagic[:])
	appendBinaryString(out, string(session.sid))
	appendBinaryString(out, session.pid)
	appendUvarint(out, uint64(len(session.rooms)))
	for _, room := range session.rooms {
		appendBinaryString(out, string(room))
	}
	appendUvarint(out, uint64(len(replay)))
	for _, packet := range replay {
		appendBinaryBytes(out, packet.packet)
		attachments := packet.attachmentViews()
		appendUvarint(out, uint64(len(attachments)))
		for _, attachment := range attachments {
			appendBinaryBytes(out, attachment)
		}
	}
	return out
}

func decodeCSRResponse(namespace string, owner *pooledBytes) (*recoverySession, []recoveryPacket, error) {
	if owner == nil {
		return nil, nil, fmt.Errorf("sio cluster: empty csr response owner")
	}
	body := owner.B
	if len(body) < len(clusterCSRMagic) || !bytes.Equal(body[:len(clusterCSRMagic)], clusterCSRMagic[:]) {
		return nil, nil, fmt.Errorf("sio cluster: invalid csr response magic")
	}
	pos := len(clusterCSRMagic)
	sid, n, err := readBinaryString(body[pos:])
	if err != nil {
		return nil, nil, err
	}
	pos += n
	pid, n, err := readBinaryString(body[pos:])
	if err != nil {
		return nil, nil, err
	}
	pos += n
	roomCount, n := binary.Uvarint(body[pos:])
	if n <= 0 {
		return nil, nil, fmt.Errorf("sio cluster: invalid csr room count")
	}
	pos += n
	rooms := make([]Room, 0, int(roomCount))
	for i := 0; i < int(roomCount); i++ {
		room, n, err := readBinaryString(body[pos:])
		if err != nil {
			return nil, nil, err
		}
		pos += n
		rooms = append(rooms, Room(room))
	}
	packetCount, n := binary.Uvarint(body[pos:])
	if n <= 0 {
		return nil, nil, fmt.Errorf("sio cluster: invalid csr packet count")
	}
	pos += n
	replay := make([]recoveryPacket, 0, int(packetCount))
	for i := 0; i < int(packetCount); i++ {
		packetBytes, n, err := readBinaryBytes(body[pos:])
		if err != nil {
			releaseReplayPackets(replay)
			return nil, nil, err
		}
		pos += n
		attachmentCount, n := binary.Uvarint(body[pos:])
		if n <= 0 {
			releaseReplayPackets(replay)
			return nil, nil, fmt.Errorf("sio cluster: invalid csr attachment count")
		}
		pos += n
		var attachments *pooledByteViews
		if attachmentCount != 0 {
			attachments = acquireByteViews(int(attachmentCount))
		}
		for j := 0; j < int(attachmentCount); j++ {
			attachment, n, err := readBinaryBytes(body[pos:])
			if err != nil {
				if attachments != nil {
					attachments.Release()
				}
				releaseReplayPackets(replay)
				return nil, nil, err
			}
			pos += n
			attachments.Append(attachment)
		}
		replay = append(replay, recoveryPacket{
			namespace:        namespace,
			packet:           packetBytes,
			views:            attachments,
			releaseAfterSend: true,
		})
	}
	session := newRecoverySession(namespace, pid, SocketID(sid), rooms, time.Time{})
	if len(replay) == 0 {
		owner.Release()
	} else {
		replay[len(replay)-1].packetOwner = owner
	}
	return session, replay, nil
}

func releaseReplayPackets(replay []recoveryPacket) {
	for i := range replay {
		replay[i].release()
	}
}

func appendBinaryString(dst *pooledBytes, value string) {
	appendUvarint(dst, uint64(len(value)))
	dst.AppendString(value)
}

func appendBinaryBytes(dst *pooledBytes, value []byte) {
	appendUvarint(dst, uint64(len(value)))
	dst.AppendBytes(value)
}

func readBinaryString(data []byte) (string, int, error) {
	value, n, err := readBinaryBytes(data)
	if err != nil {
		return "", 0, err
	}
	return bytesToStringView(value), n, nil
}

func readBinaryBytes(data []byte) ([]byte, int, error) {
	size, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, 0, fmt.Errorf("sio cluster: invalid binary string length")
	}
	if size > uint64(len(data)-n) {
		return nil, 0, fmt.Errorf("sio cluster: binary string length exceeds body")
	}
	start := n
	end := start + int(size)
	return data[start:end], end, nil
}

func (b *clusterPacketBody) Attachments() [][]byte {
	if b == nil || b.attachments == nil {
		return nil
	}
	return b.attachments.V
}

func (b *clusterPacketBody) Release() {
	if b == nil {
		return
	}
	if b.attachments != nil {
		b.attachments.Release()
		b.attachments = nil
	}
	if b.owner != nil {
		b.owner.Release()
		b.owner = nil
	}
	b.packet = nil
}

func valuesToRooms(values []string) []Room {
	rooms := make([]Room, 0, len(values))
	for _, v := range values {
		rooms = append(rooms, Room(v))
	}
	return rooms
}

func (c *clusterNode) fetchLocal(nsp *Namespace, opts broadcastOptions) []socketSnapshot {
	matches := nsp.adapter.matchingSockets(opts)
	out := make([]socketSnapshot, 0, len(matches))
	for _, socket := range matches {
		out = append(out, socketSnapshot{ID: socket.ID(), Rooms: socket.Rooms()})
	}
	return out
}

func (c *clusterNode) fetchSockets(namespace string, opts broadcastOptions) []socketSnapshot {
	localNsp, ok := c.server.getNamespace(namespace)
	out := []socketSnapshot(nil)
	if ok {
		out = append(out, c.fetchLocal(localNsp, opts)...)
	}
	responses := c.postToPeers("fetch", namespace, opts, nil, nil)
	defer releaseClusterResponses(responses)
	for _, response := range responses {
		if len(response.body) == 0 {
			continue
		}
		var remote []socketSnapshot
		if err := json.Unmarshal(response.body, &remote); err != nil {
			c.server.reportError(fmt.Errorf("sio cluster: fetch response from %s decode failed: %w", response.peer, err))
			continue
		}
		out = append(out, remote...)
	}
	return out
}

func (c *clusterNode) recoverCSR(namespace, pid, offset string) (*recoverySession, []recoveryPacket, bool) {
	extra := url.Values{}
	extra.Set("pid", pid)
	extra.Set("offset", offset)
	responses := c.postToPeers("csr", namespace, broadcastOptions{}, extra, nil)
	defer releaseClusterResponses(responses)
	for i := range responses {
		response := &responses[i]
		if response.statusCode != http.StatusOK || len(response.body) == 0 {
			continue
		}
		session, replay, err := decodeCSRResponse(namespace, response.bodyOwner)
		if err != nil {
			c.server.reportError(fmt.Errorf("sio cluster: csr response from %s decode failed: %w", response.peer, err))
			continue
		}
		response.bodyOwner = nil
		return session, replay, true
	}
	return nil, nil, false
}

func (c *clusterNode) broadcast(namespace string, opts broadcastOptions, packet []byte, attachments [][]byte) {
	body, owner := encodeClusterPacket(packet, attachments)
	defer owner.Release()
	c.postToPeers("broadcast", namespace, opts, nil, body)
}

func (c *clusterNode) broadcastWithAck(namespace string, opts broadcastOptions, packet []byte, attachments [][]byte, ackID uint64) {
	extra := url.Values{}
	extra.Set("ack", strconv.FormatUint(ackID, 10))
	if c.advertiseURL != "" {
		extra.Set("origin", c.advertiseURL+c.path+"?transport=cluster")
	}
	body, owner := encodeClusterPacket(packet, attachments)
	defer owner.Release()
	responses := c.postToPeers("broadcast", namespace, opts, extra, body)
	if tracker, ok := c.server.getBroadcastAck(ackID); ok {
		for _, response := range responses {
			if expected := response.header.Get("X-Sio-Expected"); expected != "" {
				n, _ := strconv.Atoi(expected)
				tracker.addExpected(n)
			}
		}
	}
}

func (c *clusterNode) socketsJoin(namespace string, opts broadcastOptions, room []Room) {
	extra := url.Values{}
	for _, r := range room {
		extra.Add("target", string(r))
	}
	c.postToPeers("join", namespace, opts, extra, nil)
}
func (c *clusterNode) socketsLeave(namespace string, opts broadcastOptions, room []Room) {
	extra := url.Values{}
	for _, r := range room {
		extra.Add("target", string(r))
	}
	c.postToPeers("leave", namespace, opts, extra, nil)
}
func (c *clusterNode) disconnectSockets(namespace string, opts broadcastOptions, close bool) {
	extra := url.Values{}
	if close {
		extra.Set("close", "1")
	}
	c.postToPeers("disconnect", namespace, opts, extra, nil)
}
func (c *clusterNode) serverSideEmit(namespace, eventName string, v ...any) {
	encoded, err := encodeAnyArgs(v)
	if err != nil {
		c.server.reportError(fmt.Errorf("sio cluster: serverSideEmit %q encode failed: %w", eventName, err))
		return
	}
	defer encoded.Release()
	packet := acquireBytes(encodedSize(encoded) + len(eventName) + 32)
	packet.B = appendEventEncoded(packet.B, namespace, 0, false, eventName, encoded)
	body, owner := encodeClusterPacket(packet.B, encoded.BinaryViews())
	c.postToPeers("sse", namespace, broadcastOptions{}, nil, body)
	if owner != nil {
		owner.Release()
	}
	packet.Release()
}

type clusterResponse struct {
	peer       string
	statusCode int
	header     http.Header
	body       []byte
	bodyOwner  *pooledBytes
}

func releaseClusterResponses(responses []clusterResponse) {
	for _, response := range responses {
		if response.bodyOwner != nil {
			response.bodyOwner.Release()
		}
	}
}

func (c *clusterNode) postToPeers(op, namespace string, opts broadcastOptions, extra url.Values, body []byte) []clusterResponse {
	peers := c.peerSnapshot()
	responses := make([]clusterResponse, 0, len(peers))
	reply := make(chan clusterResponse, len(peers))
	submitted := 0
	for _, peer := range peers {
		q := url.Values{}
		q.Set("op", op)
		q.Set("nsp", namespace)
		for _, room := range opts.Rooms {
			q.Add("room", string(room))
		}
		for _, room := range opts.Except {
			q.Add("except", string(room))
		}
		for k, vs := range extra {
			for _, v := range vs {
				q.Add(k, v)
			}
		}
		endpoint := appendClusterQuery(peer, q)
		if c.jobs == nil {
			if response, ok := c.doPost(op, peer, endpoint, body); ok {
				responses = append(responses, response)
			}
			continue
		}
		job := clusterJob{op: op, peer: peer, endpoint: endpoint, body: body, reply: reply}
		select {
		case c.jobs <- job:
			submitted++
		case <-c.server.lc.context().Done():
			return responses
		}
	}
	for i := 0; i < submitted; i++ {
		select {
		case response := <-reply:
			if response.header != nil {
				responses = append(responses, response)
			}
		case <-c.server.lc.context().Done():
			return responses
		}
	}
	return responses
}

func (c *clusterNode) runFanoutWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-c.jobs:
			response, ok := c.doPost(job.op, job.peer, job.endpoint, job.body)
			if !ok {
				response = clusterResponse{peer: job.peer}
			}
			select {
			case job.reply <- response:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (c *clusterNode) doPost(op, peer, endpoint string, body []byte) (clusterResponse, bool) {
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		c.server.reportError(fmt.Errorf("sio cluster: create %s request to %s failed: %w", op, peer, err))
		return clusterResponse{}, false
	}
	req.Header.Set("X-Sio-Origin", c.nodeID)
	c.server.metrics.clusterRequestsSent.Add(1)
	resp, err := c.client.Do(req)
	if err != nil {
		c.server.metrics.clusterRequestsFailed.Add(1)
		c.server.reportError(fmt.Errorf("sio cluster: %s to %s failed: %w", op, peer, err))
		return clusterResponse{}, false
	}
	var responseBody []byte
	var responseOwner *pooledBytes
	if op == "fetch" || op == "csr" {
		responseOwner, err = readAllPooled(resp.Body, resp.ContentLength)
		if err != nil {
			_ = resp.Body.Close()
			c.server.metrics.clusterRequestsFailed.Add(1)
			c.server.reportError(fmt.Errorf("sio cluster: read %s response from %s failed: %w", op, peer, err))
			return clusterResponse{}, false
		}
		responseBody = responseOwner.B
	} else {
		_, _ = io.Copy(io.Discard, resp.Body)
	}
	_ = resp.Body.Close()
	if resp.StatusCode >= 300 && (op != "csr" || resp.StatusCode != http.StatusNotFound) {
		c.server.metrics.clusterResponsesFailed.Add(1)
		c.server.reportError(fmt.Errorf("sio cluster: %s to %s returned %s", op, peer, resp.Status))
	}
	return clusterResponse{peer: peer, statusCode: resp.StatusCode, header: resp.Header.Clone(), body: responseBody, bodyOwner: responseOwner}, true
}

type remoteAckForwarder struct {
	client *http.Client
	origin string
	id     uint64
}

func (f *remoteAckForwarder) accept(args JSONArrayView, _ [][]byte) {
	body := args.data
	if len(body) == 0 {
		body = emptyJSONArrayBytes
	}
	endpoint := appendClusterQuery(f.origin, url.Values{"op": {"ack"}, "id": {strconv.FormatUint(f.id, 10)}})
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return
	}
	resp, err := f.client.Do(req)
	if err == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
}

func appendClusterQuery(peer string, q url.Values) string {
	sep := "?"
	if strings.Contains(peer, "?") {
		sep = "&"
	}
	return peer + sep + q.Encode()
}
