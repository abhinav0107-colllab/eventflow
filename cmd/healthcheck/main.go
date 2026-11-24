// Command healthcheck performs an HTTP GET and exits 0 on a 2xx response.
// It exists because the distroless runtime images ship no shell or curl,
// so container HEALTHCHECKs need a static binary.
//
//	healthcheck http://127.0.0.1:8080/health
package main

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: healthcheck <url>")
		os.Exit(2)
	}
	target, err := url.Parse(os.Args[1])
	if err != nil || target.Scheme != "http" || !isLoopback(target.Hostname()) {
		fmt.Fprintln(os.Stderr, "healthcheck only probes http://127.0.0.1, http://[::1] or http://localhost")
		os.Exit(2)
	}
	client := &http.Client{Timeout: 3 * time.Second}
	// The URL is validated above to be a loopback address set by the image's
	// HEALTHCHECK, so this cannot be used to reach other hosts.
	resp, err := client.Get(target.String()) //nolint:gosec // G704: loopback-only, validated above
	if err != nil {
		fmt.Fprintln(os.Stderr, "unhealthy:", err)
		os.Exit(1)
	}
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		fmt.Fprintln(os.Stderr, "unhealthy: status", resp.StatusCode)
		os.Exit(1)
	}
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
