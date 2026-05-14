package sio

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"sync/atomic"
)

type idGenerator struct {
	node string
	seq  atomic.Uint64
}

func newIDGenerator(node string) *idGenerator {
	if node == "" {
		var b [9]byte
		if _, err := rand.Read(b[:]); err == nil {
			node = base64.RawURLEncoding.EncodeToString(b[:])
		} else {
			node = "node"
		}
	}
	return &idGenerator{node: node}
}

func (g *idGenerator) next() SocketID {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return SocketID(fmt.Sprintf("%s-%d-%s", g.node, g.seq.Add(1), base64.RawURLEncoding.EncodeToString(b[:])))
}
