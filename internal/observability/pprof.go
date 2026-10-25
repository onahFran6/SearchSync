package observability

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"time"
)

// StartPprof starts net/http/pprof on its own isolated mux, bound only to
// addr, and refuses to start unless addr's host is loopback. It is never
// shared with /metrics or any public-facing mux - the hard constraint this
// stage documents is that a bottleneck claim needs a captured CPU/heap
// profile, not a guess, and that profiling endpoint must never be
// reachable from outside the host while capturing one.
//
// Callers must only invoke this when an operator has explicitly opted in
// (each binary's <SERVICE>_PPROF_ADDR env var) - never unconditionally.
func StartPprof(addr string) (shutdown func(context.Context) error, err error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("parse pprof addr %q: %w", addr, err)
	}
	switch host {
	case "127.0.0.1", "localhost", "::1":
	default:
		return nil, fmt.Errorf("pprof addr %q must be loopback-only, got host %q", addr, host)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	server := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = server.ListenAndServe() }()

	return func(ctx context.Context) error {
		return server.Shutdown(ctx)
	}, nil
}
