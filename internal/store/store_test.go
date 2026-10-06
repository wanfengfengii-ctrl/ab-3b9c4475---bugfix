package store

import (
	"context"
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
