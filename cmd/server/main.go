// Command server runs the ocean observation sample ingestion/snapshot API.
package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"oceanwatch/internal/api"
	"oceanwatch/internal/store"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		log.Printf("ignoring invalid %s=%q, using %d", key, v, def)
	}
	return def
}

func main() {
	healthcheck := flag.Bool("healthcheck", false,
		"probe the local /healthz endpoint once and exit with the result")
	flag.Parse()
	if *healthcheck {
		os.Exit(runHealthcheck())
	}
	runServer()
}

// runHealthcheck is the in-image HEALTHCHECK probe: no shell or curl needed.
func runHealthcheck() int {
	port := env("PORT", "8080")
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/healthz")
	if err != nil {
		log.Printf("healthcheck: %v", err)
		return 1
	}
	defer io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("healthcheck: status %d", resp.StatusCode)
		return 1
	}
	return 0
}

func runServer() {
	var (
		addr          = ":" + env("PORT", "8080")
		dbPath        = env("DB_PATH", "/data/oceanwatch.db")
		pageSize      = envInt("DEFAULT_PAGE_SIZE", 50)
		maxPageSize   = envInt("MAX_PAGE_SIZE", 200)
		shutdownAfter = time.Duration(envInt("SHUTDOWN_TIMEOUT_SECONDS", 15)) * time.Second
	)

	// The cursor secret must be stable across restarts so cursors remain
	// valid. Prefer CURSOR_SECRET; otherwise persist a random secret next
	// to the database.
	secret, err := loadOrCreateSecret(env("CURSOR_SECRET", ""), dbPath+".secret")
	if err != nil {
		log.Fatalf("cursor secret: %v", err)
	}

	if err := os.MkdirAll(dataDir(dbPath), 0o755); err != nil {
		log.Fatalf("data dir: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, dbPath)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	srv := &http.Server{
		Addr:              addr,
		Handler:           api.NewServer(st, secret, api.Config{DefaultPageSize: pageSize, MaxPageSize: maxPageSize}).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		log.Printf("oceanwatch listening on %s (db=%s)", addr, dbPath)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("shutdown signal received, draining (up to %s)", shutdownAfter)
	shutCtx, cancel := context.WithTimeout(context.Background(), shutdownAfter)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
	}
}

func dataDir(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			if i == 0 {
				return "/"
			}
			return p[:i]
		}
	}
	return "."
}

func loadOrCreateSecret(inline, path string) ([]byte, error) {
	if inline != "" {
		return []byte(inline), nil
	}
	if raw, err := os.ReadFile(path); err == nil {
		// tolerate surrounding whitespace/newlines
		s := encodableTrim(raw)
		if s != "" {
			return []byte(s), nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return nil, err
	}
	s := base64.RawURLEncoding.EncodeToString(buf)
	if err := os.WriteFile(path, []byte(s+"\n"), 0o600); err != nil {
		return nil, err
	}
	return []byte(s), nil
}

func encodableTrim(b []byte) string {
	start, end := 0, len(b)
	for start < end && (b[start] == ' ' || b[start] == '\n' || b[start] == '\r' || b[start] == '\t') {
		start++
	}
	for end > start && (b[end-1] == ' ' || b[end-1] == '\n' || b[end-1] == '\r' || b[end-1] == '\t') {
		end--
	}
	return string(b[start:end])
}
