//go:build integration

package integration

import (
	"io"
	"net"
	"sync"
	"testing"
)

// faultProxy is a TCP proxy used to simulate network partitions and
// dependency outages. While disabled it severs all live connections and
// immediately closes new ones, which is how a crashed or unreachable Redis
// or PostgreSQL looks to a client.
type faultProxy struct {
	ln      net.Listener
	target  string
	mu      sync.Mutex
	enabled bool
	conns   map[net.Conn]struct{}
}

func newFaultProxy(t *testing.T, target string) *faultProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &faultProxy{ln: ln, target: target, enabled: true, conns: map[net.Conn]struct{}{}}
	go p.serve()
	t.Cleanup(func() { _ = ln.Close(); p.Disable() })
	return p
}

func (p *faultProxy) Addr() string { return p.ln.Addr().String() }

func (p *faultProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		if !p.enabled {
			p.mu.Unlock()
			_ = c.Close()
			continue
		}
		up, err := net.Dial("tcp", p.target)
		if err != nil {
			p.mu.Unlock()
			_ = c.Close()
			continue
		}
		p.conns[c], p.conns[up] = struct{}{}, struct{}{}
		p.mu.Unlock()
		go p.pipe(c, up)
		go p.pipe(up, c)
	}
}

func (p *faultProxy) pipe(dst, src net.Conn) {
	_, _ = io.Copy(dst, src)
	_ = dst.Close()
	_ = src.Close()
	p.mu.Lock()
	delete(p.conns, dst)
	delete(p.conns, src)
	p.mu.Unlock()
}

// Disable severs all connections and refuses new ones.
func (p *faultProxy) Disable() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.enabled = false
	for c := range p.conns {
		_ = c.Close()
	}
	p.conns = map[net.Conn]struct{}{}
}

// Enable restores connectivity.
func (p *faultProxy) Enable() {
	p.mu.Lock()
	p.enabled = true
	p.mu.Unlock()
}
