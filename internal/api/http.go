// Package api wires the HTTP endpoints for stream sample ingestion and
// snapshot pagination.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"oceanwatch/internal/cursor"
	"oceanwatch/internal/store"
)

// Config holds server tunables.
type Config struct {
	DefaultPageSize int
	MaxPageSize     int
}

// Server holds dependencies for the HTTP handlers.
type Server struct {
	store  *store.Store
	secret []byte
	cfg    Config
	mux    *http.ServeMux
}

// NewServer builds the router. secret signs pagination cursors and must
// remain stable across restarts for cursors to survive them.
func NewServer(st *store.Store, secret []byte, cfg Config) *Server {
	if cfg.DefaultPageSize <= 0 {
		cfg.DefaultPageSize = 50
	}
	if cfg.MaxPageSize <= 0 {
		cfg.MaxPageSize = 200
	}
	s := &Server{store: st, secret: secret, cfg: cfg, mux: http.NewServeMux()}
	s.mux.HandleFunc("/healthz", s.handleHealth)
	s.mux.HandleFunc("/api/streams/", s.handleStreamSamples)
	return s
}

// Handler exposes the router.
func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Ping(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "db_unavailable", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// path shape: /api/streams/{streamId}/samples
func parseStreamPath(path string) (string, bool) {
	const prefix = "/api/streams/"
	if !strings.HasPrefix(path, prefix) {
		return "", false
	}
	rest := strings.TrimPrefix(path, prefix)
	parts := strings.Split(rest, "/")
	if len(parts) != 2 || parts[1] != "samples" {
		return "", false
	}
	return parts[0], true
}

func (s *Server) handleStreamSamples(w http.ResponseWriter, r *http.Request) {
	streamID, ok := parseStreamPath(r.URL.Path)
	if !ok || !validID(streamID) {
		writeError(w, http.StatusNotFound, "not_found", "unknown route")
		return
	}
	switch r.Method {
	case http.MethodPost:
		s.postSamples(r.Context(), w, r, streamID)
	case http.MethodGet:
		s.getSamples(r.Context(), w, r, streamID)
	default:
		w.Header().Set("Allow", "GET, POST")
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use GET or POST")
	}
}

type incomingSample struct {
	SampleID  string `json:"sampleId"`
	Timestamp string `json:"timestamp"`
	Value     int64  `json:"value"`
}

type postRequest struct {
	Samples []incomingSample `json:"samples"`
}

func (s *Server) postSamples(ctx context.Context, w http.ResponseWriter, r *http.Request, streamID string) {
	var req postRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON body: "+err.Error())
		return
	}
	if len(req.Samples) < 1 || len(req.Samples) > 100 {
		writeError(w, http.StatusBadRequest, "bad_request",
			"samples must contain between 1 and 100 items, got "+strconv.Itoa(len(req.Samples)))
		return
	}

	in := make([]store.Input, 0, len(req.Samples))
	for i, x := range req.Samples {
		if !validID(x.SampleID) {
			writeError(w, http.StatusBadRequest, "bad_sample",
				"samples["+strconv.Itoa(i)+"].sampleId must be 1..128 non-whitespace characters")
			return
		}
		ts, err := time.Parse(time.RFC3339Nano, x.Timestamp)
		if err != nil {
			writeError(w, http.StatusBadRequest, "bad_timestamp",
				"samples["+strconv.Itoa(i)+"].timestamp must be RFC3339: "+err.Error())
			return
		}
		in = append(in, store.Input{SampleID: x.SampleID, TS: ts, Value: x.Value})
	}

	newSeq, err := s.store.InsertBatch(ctx, streamID, in)
	if err != nil {
		var ce *store.ConflictError
		if errors.As(err, &ce) {
			writeError(w, http.StatusConflict, "conflict", ce.Error())
			return
		}
		log.Printf("insert batch failed: %v", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "insert failed")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"accepted":   len(in),
		"currentSeq": newSeq,
	})
}

type outSample struct {
	SampleID  string `json:"sampleId"`
	Timestamp string `json:"timestamp"`
	Value     int64  `json:"value"`
}

func (s *Server) getSamples(ctx context.Context, w http.ResponseWriter, r *http.Request, streamID string) {
	q := r.URL.Query()
	pageSize := s.cfg.DefaultPageSize
	if raw := q.Get("pageSize"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > s.cfg.MaxPageSize {
			writeError(w, http.StatusBadRequest, "bad_page_size",
				"pageSize must be an integer between 1 and "+strconv.Itoa(s.cfg.MaxPageSize))
			return
		}
		pageSize = n
	}

	// Query-supplied bounds, canonicalized. They define the session on the
	// first request and must not change on subsequent cursor requests.
	var queryFrom, queryTo string
	if raw := q.Get("from"); raw != "" {
		t, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "bad_from", "from must be RFC3339")
			return
		}
		queryFrom = cursor.Normalize(t)
	}
	if raw := q.Get("to"); raw != "" {
		t, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "bad_to", "to must be RFC3339")
			return
		}
		queryTo = cursor.Normalize(t)
	}
	if queryFrom != "" && queryTo != "" && queryFrom >= queryTo {
		writeError(w, http.StatusBadRequest, "bad_range", "from must be before to (to is exclusive)")
		return
	}

	var (
		tok cursor.Token
		err error
	)
	if raw := q.Get("cursor"); raw != "" {
		tok, err = cursor.Parse(raw, s.secret)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_cursor", "cursor is invalid, expired or tampered")
			return
		}
		if tok.Stream != streamID {
			writeError(w, http.StatusBadRequest, "cursor_stream_mismatch",
				"cursor was issued for a different stream")
			return
		}
		// A cursor pins its session's time range. Passing new or changed
		// bounds is a client error rather than a silently altered query.
		if queryFrom != "" && queryFrom != tok.From {
			writeError(w, http.StatusBadRequest, "cursor_range_mismatch",
				"cursor session uses a different 'from'; start a new session to change the range")
			return
		}
		if queryTo != "" && queryTo != tok.To {
			writeError(w, http.StatusBadRequest, "cursor_range_mismatch",
				"cursor session uses a different 'to'; start a new session to change the range")
			return
		}
	} else {
		snap, err := s.store.CurrentSeq(ctx, streamID)
		if err != nil {
			log.Printf("current seq failed: %v", err)
			writeError(w, http.StatusInternalServerError, "internal_error", "cannot open snapshot")
			return
		}
		tok = cursor.Token{
			Stream:  streamID,
			From:    queryFrom,
			To:      queryTo,
			SnapSeq: snap,
		}
	}

	page, err := s.store.FetchPage(ctx, store.PageOptions{
		StreamID: streamID,
		From:     tok.From,
		To:       tok.To,
		SnapSeq:  tok.SnapSeq,
		AfterTS:  tok.AfterTS,
		AfterID:  tok.AfterID,
		Limit:    pageSize,
	})
	if err != nil {
		log.Printf("fetch page failed: %v", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "query failed")
		return
	}

	items := make([]outSample, 0, len(page.Items))
	for _, sm := range page.Items {
		items = append(items, outSample{SampleID: sm.SampleID, Timestamp: sm.TS, Value: sm.Value})
	}

	resp := map[string]any{
		"snapshotSeq": tok.SnapSeq,
		"items":       items,
	}
	if page.NextAfterTS != "" {
		next := tok
		next.AfterTS = page.NextAfterTS
		next.AfterID = page.NextAfterID
		resp["nextCursor"] = cursor.Sign(next, s.secret)
		resp["done"] = false
	} else {
		resp["nextCursor"] = nil
		resp["done"] = true
	}
	writeJSON(w, http.StatusOK, resp)
}

func validID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for _, r := range id {
		if r <= ' ' || r == 0x7f {
			return false
		}
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

type errBody struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, errBody{Error: code, Message: msg})
}
