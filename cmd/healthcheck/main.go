// Command healthcheck performs an HTTP GET and exits 0 on a 2xx response.
// It exists because the distroless runtime images ship no shell or curl,
// so container HEALTHCHECKs need a static binary.
//
//	healthcheck http://127.0.0.1:8080/health
package main

import (
	"fmt"
	"net/http"
	"os"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: healthcheck <url>")
		os.Exit(2)
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(os.Args[1])
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
