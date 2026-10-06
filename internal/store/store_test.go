package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	st, err := Open(context.Background(), filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func at(h, m int) time.Time {
	return time.Date(2026, 6, 1, h, m, 0, 0, time.UTC)
}

func TestInsertBatchAtomic(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)

	// Intra-batch duplicate: nothing written.
	_, err := st.InsertBatch(ctx, "s1", []Input{
		{SampleID: "a", TS: at(1, 0), Value: 1},
		{SampleID: "b", TS: at(2, 0), Value: 2},
		{SampleID: "a", TS: at(3, 0), Value: 3},
	})
	var ce *ConflictError
	if !asConflict(err, &ce) || ce.Existing {
		t.Fatalf("want intra-batch ConflictError, got %v", err)
	}

	seq, err := st.CurrentSeq(ctx, "s1")
	if err != nil || seq != 0 {
		t.Fatalf("stream must be untouched after rejection, seq=%d err=%v", seq, err)
	}

	// Successful batch assigns monotonic seqs.
	seq, err = st.InsertBatch(ctx, "s1", []Input{
		{SampleID: "a", TS: at(1, 0), Value: 1},
		{SampleID: "b", TS: at(2, 0), Value: 2},
	})
	if err != nil || seq != 2 {
		t.Fatalf("insert: seq=%d err=%v", seq, err)
	}

	// Conflict with existing: the *new* row in the batch must not persist.
	_, err = st.InsertBatch(ctx, "s1", []Input{
		{SampleID: "c", TS: at(4, 0), Value: 4},
		{SampleID: "a", TS: at(5, 0), Value: 5},
	})
	if !asConflict(err, &ce) || !ce.Existing || ce.SampleID != "a" {
		t.Fatalf("want existing conflict on a, got %v", err)
	}
	seq, _ = st.CurrentSeq(ctx, "s1")
	if seq != 2 {
		t.Fatalf("seq must stay 2, got %d", seq)
	}
	// c must be insertable now, proving it never landed.
	seq, err = st.InsertBatch(ctx, "s1", []Input{{SampleID: "c", TS: at(4, 0), Value: 4}})
	if err != nil || seq != 3 {
		t.Fatalf("post-conflict insert of c: seq=%d err=%v", seq, err)
	}
}

func TestBatchSizeLimits(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	if _, err := st.InsertBatch(ctx, "s", nil); err == nil {
		t.Fatal("empty batch must be rejected")
	}
	big := make([]Input, 101)
	for i := range big {
		big[i] = Input{SampleID: fmt.Sprintf("x%d", i), TS: at(0, 0)}
	}
	if _, err := st.InsertBatch(ctx, "s", big); err == nil {
		t.Fatal("101-item batch must be rejected")
	}
}

func TestSnapshotWalkStableUnderLateEarlyInserts(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	mk := func(ids []string, hour int) []Input {
		out := make([]Input, len(ids))
		for i, id := range ids {
			out[i] = Input{SampleID: id, TS: at(hour+i, 0), Value: int64(i)}
		}
		return out
	}

	first := []string{"s01", "s02", "s03", "s04", "s05"}
	snapSeq, err := st.InsertBatch(ctx, "walk", mk(first, 10))
	if err != nil {
		t.Fatal(err)
	}

	// Read first page of the snapshot.
	p1, err := st.FetchPage(ctx, PageOptions{StreamID: "walk", SnapSeq: snapSeq, Limit: 2})
	if err != nil || len(p1.Items) != 2 {
		t.Fatalf("page1: %v items=%d", err, len(p1.Items))
	}
	if p1.NextAfterID != "s02" {
		t.Fatalf("page1 keyset=%s", p1.NextAfterID)
	}

	// Rows with timestamps earlier than anything snapshotted arrive.
	if _, err := st.InsertBatch(ctx, "walk", []Input{
		{SampleID: "e0", TS: at(0, 0), Value: 99},
		{SampleID: "e1", TS: at(0, 1), Value: 98},
	}); err != nil {
		t.Fatal(err)
	}

	// Remaining pages must see only the original five, once each, in order.
	var got []string
	afterTS, afterID := p1.NextAfterTS, p1.NextAfterID
	for {
		p, err := st.FetchPage(ctx, PageOptions{
			StreamID: "walk", SnapSeq: snapSeq, Limit: 2,
			AfterTS: afterTS, AfterID: afterID,
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, it := range p.Items {
			got = append(got, it.SampleID)
		}
		if p.NextAfterTS == "" {
			break
		}
		afterTS, afterID = p.NextAfterTS, p.NextAfterID
	}
	want := []string{"s03", "s04", "s05"}
	if !eq(got, want) {
		t.Fatalf("tail=%v want=%v", got, want)
	}

	n, err := st.CountSnapshot(ctx, "walk", "", "", snapSeq)
	if err != nil || n != 5 {
		t.Fatalf("snapshot count=%d err=%v", n, err)
	}

	// A fresh snapshot includes the early rows and orders them first.
	curSeq, _ := st.CurrentSeq(ctx, "walk")
	if curSeq <= snapSeq {
		t.Fatalf("seq should advance: %d <= %d", curSeq, snapSeq)
	}
	fresh, err := st.FetchPage(ctx, PageOptions{StreamID: "walk", SnapSeq: curSeq, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(fresh.Items) != 7 || fresh.Items[0].SampleID != "e0" {
		ids := make([]string, len(fresh.Items))
		for i, it := range fresh.Items {
			ids[i] = it.SampleID
		}
		t.Fatalf("fresh walk ids=%v", ids)
	}
}

func TestRangeFiltering(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	in := []Input{
		{SampleID: "a", TS: at(1, 0)},
		{SampleID: "b", TS: at(2, 0)},
		{SampleID: "c", TS: at(3, 0)},
		{SampleID: "d", TS: at(4, 0)},
	}
	snapSeq, err := st.InsertBatch(ctx, "rng", in)
	if err != nil {
		t.Fatal(err)
	}
	// [02:00, 04:00) -> b, c
	n, err := st.CountSnapshot(ctx, "rng",
		"2026-06-01T02:00:00Z", "2026-06-01T04:00:00Z", snapSeq)
	if err != nil || n != 2 {
		t.Fatalf("count=%d err=%v", n, err)
	}
	p, err := st.FetchPage(ctx, PageOptions{
		StreamID: "rng", SnapSeq: snapSeq, Limit: 10,
		From: "2026-06-01T02:00:00Z", To: "2026-06-01T04:00:00Z",
	})
	if err != nil || len(p.Items) != 2 || p.Items[0].SampleID != "b" || p.Items[1].SampleID != "c" {
		t.Fatalf("range page=%v err=%v", p.Items, err)
	}
}

func TestTieBreakBySampleID(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	_, err := st.InsertBatch(ctx, "tie", []Input{
		{SampleID: "z", TS: at(1, 0)},
		{SampleID: "a", TS: at(1, 0)},
		{SampleID: "m", TS: at(1, 0)},
	})
	if err != nil {
		t.Fatal(err)
	}
	cur, _ := st.CurrentSeq(ctx, "tie")
	p, err := st.FetchPage(ctx, PageOptions{StreamID: "tie", SnapSeq: cur, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{p.Items[0].SampleID, p.Items[1].SampleID, p.Items[2].SampleID}; !eq(got, []string{"a", "m", "z"}) {
		t.Fatalf("tie order=%v", got)
	}
}

// Mixed fractional-second precisions must order by true instant: the
// variable-width RFC3339Nano text form sorts ".1Z" before "Z".
func TestMixedPrecisionOrderingRangeAndKeyset(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)

	exact := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	later := exact.Add(100 * time.Millisecond)
	snapSeq, err := st.InsertBatch(ctx, "mix", []Input{
		{SampleID: "exact", TS: exact, Value: 1},
		{SampleID: "later", TS: later, Value: 2},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Unbounded walk: exact (whole second) before later (.1).
	p, err := st.FetchPage(ctx, PageOptions{StreamID: "mix", SnapSeq: snapSeq, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{p.Items[0].SampleID, p.Items[1].SampleID}; !eq(got, []string{"exact", "later"}) {
		t.Fatalf("unbounded order=%v", got)
	}
	if p.Items[0].TS != "2027-01-01T00:00:00Z" || p.Items[1].TS != "2027-01-01T00:00:00.1Z" {
		t.Fatalf("display form changed: %q %q", p.Items[0].TS, p.Items[1].TS)
	}

	// Narrow half-open range covering both instants returns both.
	from, to := "2027-01-01T00:00:00Z", "2027-01-01T00:00:00.2Z"
	p, err = st.FetchPage(ctx, PageOptions{StreamID: "mix", SnapSeq: snapSeq, Limit: 10, From: from, To: to})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Items) != 2 {
		t.Fatalf("range [%s,%s) returned %d items", from, to, len(p.Items))
	}
	if n, err := st.CountSnapshot(ctx, "mix", from, to, snapSeq); err != nil || n != 2 {
		t.Fatalf("count=%d err=%v", n, err)
	}

	// A range ending between the two instants keeps only the whole-second one.
	p, err = st.FetchPage(ctx, PageOptions{
		StreamID: "mix", SnapSeq: snapSeq, Limit: 10,
		From: "2026-12-31T23:59:59.5Z", To: "2027-01-01T00:00:00.05Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Items) != 1 || p.Items[0].SampleID != "exact" {
		t.Fatalf("tight range items=%v", p.Items)
	}

	// Keyset walk with the page boundary inside the same second.
	p1, err := st.FetchPage(ctx, PageOptions{StreamID: "mix", SnapSeq: snapSeq, Limit: 1})
	if err != nil || len(p1.Items) != 1 || p1.Items[0].SampleID != "exact" {
		t.Fatalf("page1=%v err=%v", p1.Items, err)
	}
	if p1.NextAfterTS != "2027-01-01T00:00:00Z" || p1.NextAfterID != "exact" {
		t.Fatalf("page1 keyset=(%q,%q)", p1.NextAfterTS, p1.NextAfterID)
	}
	p2, err := st.FetchPage(ctx, PageOptions{
		StreamID: "mix", SnapSeq: snapSeq, Limit: 1,
		AfterTS: p1.NextAfterTS, AfterID: p1.NextAfterID,
	})
	if err != nil || len(p2.Items) != 1 || p2.Items[0].SampleID != "later" {
		t.Fatalf("page2=%v err=%v", p2.Items, err)
	}
	if p2.NextAfterTS != "" {
		t.Fatalf("page2 must end the walk, keyset=(%q,%q)", p2.NextAfterTS, p2.NextAfterID)
	}
}

// Identical instants tie-break by sampleId; a single nanosecond of
// difference still orders chronologically.
func TestNanosecondOrderingAndTieBreak(t *testing.T) {
	ctx := context.Background()
	st := newTestStore(t)
	base := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	snapSeq, err := st.InsertBatch(ctx, "tie2", []Input{
		{SampleID: "b", TS: base, Value: 1},
		{SampleID: "a", TS: base, Value: 2},
		{SampleID: "c", TS: base.Add(time.Nanosecond), Value: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	p, err := st.FetchPage(ctx, PageOptions{StreamID: "tie2", SnapSeq: snapSeq, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	got := []string{p.Items[0].SampleID, p.Items[1].SampleID, p.Items[2].SampleID}
	if !eq(got, []string{"a", "b", "c"}) {
		t.Fatalf("tie order=%v", got)
	}
}

// Databases written before the sort-key column existed must be migrated:
// ts_key backfilled, old index replaced, ordering chronological.
func TestMigrateLegacyRows(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy.db")

	// Build a pre-fix database: no ts_key column, old text-ordered index.
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	legacy := []string{
		`CREATE TABLE samples (
			stream_id  TEXT NOT NULL,
			sample_id  TEXT NOT NULL,
			ts         TEXT NOT NULL,
			value      INTEGER NOT NULL,
			seq        INTEGER NOT NULL,
			PRIMARY KEY (stream_id, sample_id)
		) WITHOUT ROWID`,
		`CREATE INDEX idx_samples_walk ON samples (stream_id, ts, sample_id, seq)`,
		`CREATE TABLE stream_meta (
			stream_id TEXT PRIMARY KEY,
			next_seq  INTEGER NOT NULL
		) WITHOUT ROWID`,
		`INSERT INTO samples (stream_id, sample_id, ts, value, seq) VALUES
			('s', 'exact', '2027-01-01T00:00:00Z', 1, 1),
			('s', 'later', '2027-01-01T00:00:00.1Z', 2, 2)`,
		`INSERT INTO stream_meta (stream_id, next_seq) VALUES ('s', 2)`,
	}
	for _, q := range legacy {
		if _, err := raw.ExecContext(ctx, q); err != nil {
			t.Fatalf("legacy setup: %v", err)
		}
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	st, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("open legacy db: %v", err)
	}
	defer func() { _ = st.Close() }()

	p, err := st.FetchPage(ctx, PageOptions{StreamID: "s", SnapSeq: 2, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{p.Items[0].SampleID, p.Items[1].SampleID}; !eq(got, []string{"exact", "later"}) {
		t.Fatalf("migrated order=%v", got)
	}
	// Range and keyset comparisons work on backfilled rows too.
	p, err = st.FetchPage(ctx, PageOptions{
		StreamID: "s", SnapSeq: 2, Limit: 10,
		From: "2027-01-01T00:00:00Z", To: "2027-01-01T00:00:00.2Z",
	})
	if err != nil || len(p.Items) != 2 {
		t.Fatalf("migrated range items=%v err=%v", p.Items, err)
	}
	// New inserts after migration keep working.
	if _, err := st.InsertBatch(ctx, "s", []Input{
		{SampleID: "new", TS: time.Date(2027, 1, 1, 0, 0, 0, 500, time.UTC)},
	}); err != nil {
		t.Fatal(err)
	}
	p, err = st.FetchPage(ctx, PageOptions{StreamID: "s", SnapSeq: 3, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	got := []string{p.Items[0].SampleID, p.Items[1].SampleID, p.Items[2].SampleID}
	if !eq(got, []string{"exact", "new", "later"}) {
		t.Fatalf("post-migration order=%v", got)
	}
}

func asConflict(err error, target **ConflictError) bool {
	if err == nil {
		return false
	}
	ce, ok := err.(*ConflictError)
	if !ok {
		return false
	}
	*target = ce
	return true
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
