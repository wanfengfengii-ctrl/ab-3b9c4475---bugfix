package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"oceanwatch/internal/store"
)

func newTestServer(t *testing.T) (*Server, *store.Store) {
	t.Helper()
	st, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "api.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return NewServer(st, []byte("test-secret"), Config{DefaultPageSize: 2, MaxPageSize: 10}), st
}

func do(t *testing.T, h http.Handler, method, target, body string) (int, map[string]any) {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, target, r)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var out map[string]any
	if rec.Body.Len() > 0 {
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("decode %q: %v", rec.Body.String(), err)
		}
	}
	return rec.Code, out
}

func mustItems(t *testing.T, body map[string]any) []any {
	t.Helper()
	items, ok := body["items"].([]any)
	if !ok {
		t.Fatalf("no items in %v", body)
	}
	return items
}

func itemID(m map[string]any) string { return m["sampleId"].(string) }

func TestHealth(t *testing.T) {
	srv, _ := newTestServer(t)
	code, body := do(t, srv.Handler(), "GET", "/healthz", "")
	if code != 200 || body["status"] != "ok" {
		t.Fatalf("health code=%d body=%v", code, body)
	}
}

func TestPostValidationAndConflict(t *testing.T) {
	srv, _ := newTestServer(t)
	h := srv.Handler()

	code, _ := do(t, h, "POST", "/api/streams/s1/samples", `{"samples":[]}`)
	if code != 400 {
		t.Fatalf("empty batch: %d", code)
	}
	code, _ = do(t, h, "POST", "/api/streams/s1/samples", `{"samples":[
		{"sampleId":"a","timestamp":"2026-01-01T00:00:00Z","value":1},
		{"sampleId":"a","timestamp":"2026-01-01T00:01:00Z","value":2}]}`)
	if code != 409 {
		t.Fatalf("dup batch: %d", code)
	}
	code, _ = do(t, h, "POST", "/api/streams/s1/samples", `{"samples":[
		{"sampleId":"a","timestamp":"2026-01-01T00:00:00Z","value":1}]}`)
	if code != 201 {
		t.Fatalf("valid post: %d", code)
	}
	code, _ = do(t, h, "POST", "/api/streams/s1/samples", `{"samples":[
		{"sampleId":"b","timestamp":"2026-01-01T00:02:00Z","value":2},
		{"sampleId":"a","timestamp":"2026-01-01T00:03:00Z","value":3}]}`)
	if code != 409 {
		t.Fatalf("conflict batch: %d", code)
	}
	// b from the rejected batch must be insertable.
	code, _ = do(t, h, "POST", "/api/streams/s1/samples", `{"samples":[
		{"sampleId":"b","timestamp":"2026-01-01T00:02:00Z","value":2}]}`)
	if code != 201 {
		t.Fatalf("b should not exist: %d", code)
	}
}

func TestSnapshotPagination(t *testing.T) {
	srv, _ := newTestServer(t)
	h := srv.Handler()

	code, _ := do(t, h, "POST", "/api/streams/snap/samples", `{"samples":[
		{"sampleId":"s1","timestamp":"2026-06-01T01:00:00Z","value":1},
		{"sampleId":"s2","timestamp":"2026-06-01T02:00:00Z","value":2},
		{"sampleId":"s3","timestamp":"2026-06-01T03:00:00Z","value":3},
		{"sampleId":"s4","timestamp":"2026-06-01T04:00:00Z","value":4}]}`)
	if code != 201 {
		t.Fatalf("seed: %d", code)
	}

	// Page 1.
	code, body := do(t, h, "GET", "/api/streams/snap/samples?pageSize=2", "")
	if code != 200 {
		t.Fatalf("page1: %d", code)
	}
	seq := body["snapshotSeq"].(float64)
	if seq != 4 {
		t.Fatalf("snapshotSeq=%v want 4", seq)
	}
	if body["done"].(bool) {
		t.Fatal("page1 must not be done")
	}
	cursor := body["nextCursor"].(string)
	if ids := idsOf(mustItems(t, body)); !eq(ids, []string{"s1", "s2"}) {
		t.Fatalf("page1 ids=%v", ids)
	}

	// Earlier-timestamp rows land between pages.
	code, _ = do(t, h, "POST", "/api/streams/snap/samples", `{"samples":[
		{"sampleId":"e1","timestamp":"2020-01-01T00:00:00Z","value":9}]}`)
	if code != 201 {
		t.Fatalf("early insert: %d", code)
	}

	// Page 2 continues the pinned snapshot.
	code, body = do(t, h, "GET", "/api/streams/snap/samples?pageSize=2&cursor="+cursor, "")
	if code != 200 || body["snapshotSeq"].(float64) != seq {
		t.Fatalf("page2 code=%d body=%v", code, body)
	}
	if ids := idsOf(mustItems(t, body)); !eq(ids, []string{"s3", "s4"}) {
		t.Fatalf("page2 ids=%v", ids)
	}
	if !body["done"].(bool) || body["nextCursor"] != nil {
		t.Fatalf("page2 should be done: %v", body)
	}
}

func TestCursorErrors(t *testing.T) {
	srv, _ := newTestServer(t)
	h := srv.Handler()
	do(t, h, "POST", "/api/streams/x/samples", `{"samples":[
		{"sampleId":"a","timestamp":"2026-01-01T00:00:00Z","value":1},
		{"sampleId":"b","timestamp":"2026-01-02T00:00:00Z","value":2},
		{"sampleId":"c","timestamp":"2026-01-03T00:00:00Z","value":3}]}`)

	_, body := do(t, h, "GET", "/api/streams/x/samples?pageSize=2", "")
	cur := body["nextCursor"].(string)

	// Tampered.
	last := cur[len(cur)-1]
	repl := "A"
	if last == 'A' {
		repl = "B"
	}
	code, body := do(t, h, "GET", "/api/streams/x/samples?cursor="+cur[:len(cur)-1]+repl, "")
	if code != 400 || body["error"] != "invalid_cursor" {
		t.Fatalf("tamper code=%d body=%v", code, body)
	}

	// Cross-stream.
	code, body = do(t, h, "GET", "/api/streams/y/samples?cursor="+cur, "")
	if code != 400 || body["error"] != "cursor_stream_mismatch" {
		t.Fatalf("cross-stream code=%d body=%v", code, body)
	}

	// Range change: create a ranged session then alter 'to'.
	_, rbody := do(t, h, "GET",
		"/api/streams/x/samples?pageSize=1&from=2026-01-01T00:00:00Z&to=2026-01-03T00:00:00Z", "")
	rcur := rbody["nextCursor"].(string)
	code, body = do(t, h, "GET",
		"/api/streams/x/samples?cursor="+rcur+"&to=2027-01-01T00:00:00Z", "")
	if code != 400 || body["error"] != "cursor_range_mismatch" {
		t.Fatalf("range change code=%d body=%v", code, body)
	}
	// Identical bounds are allowed on cursor requests.
	code, _ = do(t, h, "GET",
		"/api/streams/x/samples?cursor="+rcur+
			"&from=2026-01-01T00:00:00Z&to=2026-01-03T00:00:00Z", "")
	if code != 200 {
		t.Fatalf("identical bounds should be legal, code=%d", code)
	}
}

func TestEmptyStreamSnapshot(t *testing.T) {
	srv, _ := newTestServer(t)
	code, body := do(t, srv.Handler(), "GET", "/api/streams/ghost/samples", "")
	if code != 200 || !body["done"].(bool) || body["snapshotSeq"].(float64) != 0 {
		t.Fatalf("empty snapshot: code=%d body=%v", code, body)
	}
}

func idsOf(items []any) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = itemID(it.(map[string]any))
	}
	return out
}

func eq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
