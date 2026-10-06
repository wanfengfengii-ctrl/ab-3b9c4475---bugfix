// Command verify is a one-shot smoke test client. It assumes a clean server
// start and exercises: batch validation, atomic rejection, conflicts, stable
// snapshot pagination while earlier-timestamp rows are inserted mid-walk,
// range filtering and cursor error cases.
//
// -mode=smoke (default) runs the full suite and additionally prepares a
// paused session, writing its cursor to a shared volume.
// -mode=resume continues that session after the server has been restarted,
// proving cursors survive restarts and still pin the original snapshot.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var (
	statePath = envOr("RESTART_STATE_PATH", "/shared/restart.json")
	base      = strings.TrimSuffix(envOr("BASE_URL", "http://app:8080"), "/")
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type failure struct{ name, detail string }

type runner struct {
	failures []failure
}

func (r *runner) check(name string, ok bool, detail string, args ...any) {
	if ok {
		fmt.Printf("PASS %s\n", name)
		return
	}
	msg := fmt.Sprintf(detail, args...)
	fmt.Printf("FAIL %s: %s\n", name, msg)
	r.failures = append(r.failures, failure{name, msg})
}

func main() {
	mode := flag.String("mode", "smoke", "smoke or resume")
	flag.Parse()

	cli := &client{http: &http.Client{Timeout: 10 * time.Second}}
	if err := waitHealthy(cli, 60*time.Second); err != nil {
		fmt.Printf("FAIL server never became healthy: %v\n", err)
		os.Exit(1)
	}

	r := &runner{}
	switch *mode {
	case "smoke":
		runSmoke(r, cli)
		prepareRestart(r, cli)
	case "resume":
		runResume(r, cli)
	default:
		fmt.Printf("unknown mode %q\n", *mode)
		os.Exit(2)
	}

	if len(r.failures) > 0 {
		fmt.Printf("\nverify: %d check(s) failed\n", len(r.failures))
		os.Exit(1)
	}
	fmt.Println("\nverify: all checks passed")
}

// ---------- client ----------

type client struct{ http *http.Client }

type sampleIn struct {
	SampleID  string `json:"sampleId"`
	Timestamp string `json:"timestamp"`
	Value     int64  `json:"value"`
}

type postBody struct {
	Samples []sampleIn `json:"samples"`
}

type postResp struct {
	Accepted   int   `json:"accepted"`
	CurrentSeq int64 `json:"currentSeq"`
}

type sampleOut struct {
	SampleID  string `json:"sampleId"`
	Timestamp string `json:"timestamp"`
	Value     int64  `json:"value"`
}

type pageResp struct {
	SnapshotSeq int64       `json:"snapshotSeq"`
	Items       []sampleOut `json:"items"`
	NextCursor  *string     `json:"nextCursor"`
	Done        bool        `json:"done"`
	Error       string      `json:"error"`
	Message     string      `json:"message"`
	raw         string
	status      int
}

func (c *client) post(stream string, samples []sampleIn) (int, *postResp, map[string]any) {
	body, _ := json.Marshal(postBody{Samples: samples})
	resp, err := c.http.Post(base+"/api/streams/"+stream+"/samples", "application/json", bytes.NewReader(body))
	if err != nil {
		fmt.Printf("POST error: %v\n", err)
		return -1, nil, nil
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var pr postResp
	_ = json.Unmarshal(raw, &pr)
	var generic map[string]any
	_ = json.Unmarshal(raw, &generic)
	return resp.StatusCode, &pr, generic
}

func (c *client) get(stream, query string) pageResp {
	url := base + "/api/streams/" + stream + "/samples"
	if query != "" {
		url += "?" + query
	}
	resp, err := c.http.Get(url)
	if err != nil {
		return pageResp{status: -1, raw: err.Error()}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var p pageResp
	_ = json.Unmarshal(raw, &p)
	p.raw = string(raw)
	p.status = resp.StatusCode
	return p
}

func waitHealthy(c *client, within time.Duration) error {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		resp, err := c.http.Get(base + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("timeout after %s", within)
}

// ---------- smoke ----------

func runSmoke(r *runner, c *client) {
	// 1. Basic validation.
	st, _, _ := c.post("validation", []sampleIn{})
	r.check("post empty batch rejected", st == 400, "status=%d", st)

	big := make([]sampleIn, 101)
	for i := range big {
		big[i] = sampleIn{SampleID: fmt.Sprintf("x%03d", i), Timestamp: "2026-01-01T00:00:00Z", Value: int64(i)}
	}
	st, _, _ = c.post("validation", big)
	r.check("post 101-item batch rejected", st == 400, "status=%d", st)

	st, _, body := c.post("validation", []sampleIn{{SampleID: "ok", Timestamp: "not-a-time", Value: 1}})
	r.check("post bad timestamp rejected", st == 400, "status=%d body=%v", st, body)

	// 2. Intra-batch duplicate rejects the WHOLE batch.
	st, _, _ = c.post("atomic", []sampleIn{
		{SampleID: "a1", Timestamp: "2026-03-01T00:00:00Z", Value: 1},
		{SampleID: "a2", Timestamp: "2026-03-01T00:01:00Z", Value: 2},
		{SampleID: "a1", Timestamp: "2026-03-01T00:02:00Z", Value: 3},
	})
	r.check("intra-batch duplicate rejected with 409", st == 409, "status=%d", st)
	for _, id := range []string{"a1", "a2"} {
		st, _, _ = c.post("atomic", []sampleIn{{SampleID: id, Timestamp: "2026-03-01T00:00:00Z", Value: 1}})
		r.check("rejected batch left no partial write ("+id+")", st == 201, "status=%d", st)
	}

	// 3. Conflict with existing id rejects the whole batch, no partials.
	st, _, _ = c.post("conflict", []sampleIn{{SampleID: "k1", Timestamp: "2026-03-01T00:00:00Z", Value: 9}})
	r.check("seed conflict stream", st == 201, "status=%d", st)
	st, _, _ = c.post("conflict", []sampleIn{
		{SampleID: "n1", Timestamp: "2026-03-02T00:00:00Z", Value: 1},
		{SampleID: "k1", Timestamp: "2026-03-02T00:01:00Z", Value: 2},
		{SampleID: "n2", Timestamp: "2026-03-02T00:02:00Z", Value: 3},
	})
	r.check("conflicting batch rejected with 409", st == 409, "status=%d", st)
	for _, id := range []string{"n1", "n2"} {
		st, _, _ = c.post("conflict", []sampleIn{{SampleID: id, Timestamp: "2026-03-02T00:00:00Z", Value: 1}})
		r.check("conflicting batch left no partial write ("+id+")", st == 201, "status=%d", st)
	}

	// 4. Stable snapshot: 30 samples, walk with pageSize 7, insert earlier
	// rows mid-walk; they must never show up and seq must stay fixed.
	const n = 30
	initial := make([]sampleIn, 0, n)
	expectedIDs := make([]string, 0, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("s%02d", i)
		initial = append(initial, sampleIn{
			SampleID:  id,
			Timestamp: fmt.Sprintf("2026-06-%02dT%02d:00:00Z", 1+i/24, i%24),
			Value:     int64(i),
		})
		expectedIDs = append(expectedIDs, id)
	}
	// split across two batches, both must land
	st, pr, _ := c.post("snap", initial[:20])
	r.check("seed batch 1 accepted", st == 201 && pr.Accepted == 20, "status=%d", st)
	st, pr, _ = c.post("snap", initial[20:])
	r.check("seed batch 2 accepted", st == 201 && pr.Accepted == 10, "status=%d", st)

	var (
		cursor    string
		got       []sampleOut
		snapSeq   int64 = -1
		pageCount int
	)
	for {
		q := "pageSize=7"
		if cursor != "" {
			q += "&cursor=" + cursor
		}
		p := c.get("snap", q)
		r.check("snapshot page status 200", p.status == 200, "status=%d raw=%s", p.status, p.raw)
		if p.status != 200 {
			break
		}
		if snapSeq == -1 {
			snapSeq = p.SnapshotSeq
		}
		r.check("snapshotSeq invariant", p.SnapshotSeq == snapSeq,
			"page=%d got=%d want=%d", pageCount, p.SnapshotSeq, snapSeq)
		got = append(got, p.Items...)
		pageCount++

		// Inject earlier-timestamp samples right in the middle of the walk.
		if pageCount == 2 {
			early := []sampleIn{
				{SampleID: "e0", Timestamp: "2020-01-01T00:00:00Z", Value: 100},
				{SampleID: "e1", Timestamp: "2020-01-01T00:01:00Z", Value: 101},
				{SampleID: "e2", Timestamp: "2020-01-01T00:02:00Z", Value: 102},
			}
			st, pr, _ := c.post("snap", early)
			r.check("early rows inserted mid-walk", st == 201 && pr.Accepted == 3,
				"status=%d", st)
		}

		if p.Done {
			r.check("done page has no nextCursor", p.NextCursor == nil, "cursor present on done")
			break
		}
		r.check("non-done page has nextCursor", p.NextCursor != nil, "missing cursor")
		if p.NextCursor == nil {
			break
		}
		cursor = *p.NextCursor
		if pageCount > 20 {
			r.check("walk terminates", false, "too many pages")
			break
		}
	}
	r.check("walked ceil(30/7)=5 pages", pageCount == 5, "pages=%d", pageCount)
	r.check("snapshot returned exactly 30 items", len(got) == 30, "got=%d", len(got))

	gotIDs := make([]string, 0, len(got))
	for _, s := range got {
		gotIDs = append(gotIDs, s.SampleID)
	}
	r.check("snapshot contents exactly the seeded ids", eqStrings(gotIDs, expectedIDs),
		"got=%v want=%v", gotIDs, expectedIDs)
	r.check("snapshot ordered by (timestamp, sampleId)", sortedByTSID(got), "order broken")

	// A fresh session after the early inserts must include them.
	fresh := c.get("snap", "pageSize=100")
	r.check("fresh session status 200", fresh.status == 200, "raw=%s", fresh.raw)
	r.check("fresh session includes late early-timestamp rows",
		len(fresh.Items) == 33 && fresh.SnapshotSeq > snapSeq,
		"items=%d seq=%d oldSeq=%d", len(fresh.Items), fresh.SnapshotSeq, snapSeq)
	r.check("fresh session orders early rows first",
		len(fresh.Items) >= 3 && fresh.Items[0].SampleID == "e0" && fresh.Items[2].SampleID == "e2",
		"first=%v", fresh.Items[:min(3, len(fresh.Items))])

	// 5. Time range pinned into the session. A new session opened now sees
	// the current sequence (30 seeded + 3 early = 33); the range must
	// exclude the 2020 early rows while including all seeded rows.
	rangeIDs, rangeSeq, ok := walkAll(c, "snap", "pageSize=10&from=2026-06-01T00:00:00Z&to=2026-06-03T00:00:00Z")
	r.check("range walk ok", ok, "range walk failed")
	r.check("range excludes early rows, keeps every seeded row",
		len(rangeIDs) == 30 && eqStrings(rangeIDs, expectedIDs),
		"got=%d ids=%v", len(rangeIDs), rangeIDs)
	r.check("range session pins its own current snapshotSeq",
		rangeSeq == 33 && rangeSeq > snapSeq, "got=%d", rangeSeq)

	// 6. Cursor error cases.
	p := c.get("snap", "pageSize=7")
	good := ""
	if p.NextCursor != nil {
		good = *p.NextCursor
	}
	r.check("obtain cursor for tamper tests", good != "", "no cursor")
	if good != "" {
		tampered := good
		if strings.HasSuffix(tampered, "A") || strings.HasSuffix(tampered, "a") {
			tampered = tampered[:len(tampered)-1] + "B"
		} else {
			tampered = tampered[:len(tampered)-1] + "A"
		}
		p = c.get("snap", "pageSize=7&cursor="+tampered)
		r.check("tampered cursor rejected", p.status == 400 && p.Error == "invalid_cursor",
			"status=%d err=%s", p.status, p.Error)

		p = c.get("other-stream", "pageSize=7&cursor="+good)
		r.check("cross-stream cursor rejected", p.status == 400 && p.Error == "cursor_stream_mismatch",
			"status=%d err=%s", p.status, p.Error)
	}

	// Range-change cursor rejected.
	rp := c.get("snap", "pageSize=5&from=2026-06-01T00:00:00Z&to=2026-06-01T12:00:00Z")
	if rp.NextCursor != nil {
		p = c.get("snap", "cursor="+*rp.NextCursor+"&to=2026-07-01T00:00:00Z")
		r.check("changed 'to' on cursor session rejected", p.status == 400 && p.Error == "cursor_range_mismatch",
			"status=%d err=%s", p.status, p.Error)
		p = c.get("snap", "cursor="+*rp.NextCursor+"&from=2025-01-01T00:00:00Z")
		r.check("changed 'from' on cursor session rejected", p.status == 400 && p.Error == "cursor_range_mismatch",
			"status=%d err=%s", p.status, p.Error)
		// Same bounds echoed back remain legal.
		p = c.get("snap", "cursor="+*rp.NextCursor+
			"&from=2026-06-01T00:00:00Z&to=2026-06-01T12:00:00Z")
		r.check("repeating identical bounds stays legal", p.status == 200, "status=%d raw=%s", p.status, p.raw)
	} else {
		r.check("obtain range cursor", false, "no cursor raw=%s", rp.raw)
	}
}

// walkAll drains a session from its initial query string and returns ids.
// It asserts every page of the new session reports one identical snapshotSeq.
func walkAll(c *client, stream, initialQuery string) ([]string, int64, bool) {
	var ids []string
	var seq int64
	cursor := ""
	// First request uses the full query; later pages keep only cursor.
	for pages := 0; pages < 100; pages++ {
		q := initialQuery
		if cursor != "" {
			q = "cursor=" + cursor
		}
		p := c.get(stream, q)
		if p.status != 200 {
			return nil, 0, false
		}
		if pages == 0 {
			seq = p.SnapshotSeq
		} else if p.SnapshotSeq != seq {
			fmt.Printf("FAIL %s: snapshotSeq drifted within session %d -> %d\n",
				initialQuery, seq, p.SnapshotSeq)
			return nil, 0, false
		}
		for _, it := range p.Items {
			ids = append(ids, it.SampleID)
		}
		if p.Done {
			return ids, seq, true
		}
		if p.NextCursor == nil {
			return nil, 0, false
		}
		cursor = *p.NextCursor
	}
	return nil, 0, false
}

// ---------- restart persistence ----------

type restartState struct {
	Stream      string   `json:"stream"`
	Cursor      string   `json:"cursor"`
	Seen        []string `json:"seen"`
	SnapshotSeq int64    `json:"snapshotSeq"`
	Expected    []string `json:"expected"`
	PageSize    int      `json:"pageSize"`
}

func prepareRestart(r *runner, c *client) {
	const total = 25
	expected := make([]string, 0, total)
	batch := make([]sampleIn, 0, total)
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("r%02d", i)
		expected = append(expected, id)
		batch = append(batch, sampleIn{
			SampleID:  id,
			Timestamp: fmt.Sprintf("2026-09-01T%02d:%02d:00Z", i/60, i%60),
			Value:     int64(700 + i),
		})
	}
	st, _, _ := c.post("rst", batch)
	r.check("restart: seed accepted", st == 201, "status=%d", st)

	// Read two pages of size 7 (14 items), then park the cursor.
	p1 := c.get("rst", "pageSize=7")
	p2 := c.get("rst", "pageSize=7&cursor="+*p1.NextCursor)
	r.check("restart: first two pages ok",
		p1.status == 200 && p2.status == 200 && p2.NextCursor != nil,
		"p1=%d p2=%d", p1.status, p2.status)
	if p2.NextCursor == nil {
		return
	}
	seen := make([]string, 0, 14)
	for _, p := range [][]sampleOut{p1.Items, p2.Items} {
		for _, it := range p {
			seen = append(seen, it.SampleID)
		}
	}
	derr := dumpState(restartState{
		Stream:      "rst",
		Cursor:      *p2.NextCursor,
		Seen:        seen,
		SnapshotSeq: p1.SnapshotSeq,
		Expected:    expected,
		PageSize:    7,
	})
	r.check("restart: checkpoint written", derr == nil, "err=%v", derr)
}

func runResume(r *runner, c *client) {
	raw, err := os.ReadFile(statePath)
	r.check("resume: checkpoint present", err == nil, "err=%v", err)
	if err != nil {
		return
	}
	var st restartState
	r.check("resume: checkpoint parses", json.Unmarshal(raw, &st) == nil, "raw=%s", raw)
	if st.Cursor == "" {
		return
	}

	// Insert earlier data during the restart window: must stay invisible.
	code, _, _ := c.post(st.Stream, []sampleIn{{
		SampleID: "re-early", Timestamp: "2019-01-01T00:00:00Z", Value: 1,
	}})
	r.check("resume: early insert after restart accepted", code == 201, "status=%d", code)

	cursor := st.Cursor
	seen := append([]string{}, st.Seen...)
	pages := 0
	for {
		p := c.get(st.Stream, "pageSize=7&cursor="+cursor)
		r.check("resume: page status 200", p.status == 200, "status=%d raw=%s", p.status, p.raw)
		if p.status != 200 {
			return
		}
		r.check("resume: snapshotSeq unchanged across restart", p.SnapshotSeq == st.SnapshotSeq,
			"got=%d want=%d", p.SnapshotSeq, st.SnapshotSeq)
		for _, it := range p.Items {
			seen = append(seen, it.SampleID)
		}
		pages++
		if p.Done {
			break
		}
		if p.NextCursor == nil || pages > 20 {
			r.check("resume: terminates cleanly", false, "pages=%d", pages)
			return
		}
		cursor = *p.NextCursor
	}
	r.check("resume: exactly every snapshot item once", eqStrings(seen, st.Expected),
		"got=%v want=%v", seen, st.Expected)
}

func dumpState(s restartState) error {
	if dir := filepath.Dir(statePath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	raw, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(statePath, raw, 0o644)
}

// ---------- helpers ----------

func eqStrings(a, b []string) bool {
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

func sortedByTSID(items []sampleOut) bool {
	for i := 1; i < len(items); i++ {
		if items[i-1].Timestamp > items[i].Timestamp ||
			(items[i-1].Timestamp == items[i].Timestamp && items[i-1].SampleID >= items[i].SampleID) {
			return false
		}
	}
	return true
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
