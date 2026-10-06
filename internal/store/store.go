// Package store is the persistence layer for stream samples.
//
// It serializes all writes through a single connection guarded by a mutex so
// that a batch POST is atomic: all rows land, or none do, and the monotonic
// ingestion sequence (seq) can never be observed half-advanced. Reads use a
// separate connection and see only rows with seq <= snapshotSeq, which makes
// a paginated session a stable snapshot even when newer (including earlier
// timestamped) samples arrive mid-walk.
//
// Timestamps are stored in a fixed-width canonical form (UTC, always nine
// fractional digits) so that the text order of the ts column is exactly the
// temporal order, no matter which fractional-second precision clients used.
// Bounds and keyset positions arriving in any RFC3339 form — including ones
// issued by older versions — are canonicalized at the query boundary, and
// rows are converted back to canonical RFC3339Nano for display.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Sample is one observation inside a stream.
type Sample struct {
	StreamID string
	SampleID string
	TS       string // canonical RFC3339Nano UTC (display form)
	Value    int64
	Seq      int64
}

// sortKeyLayout is the fixed-width timestamp form stored in the ts column.
// Every legal RFC3339 instant maps to exactly one 30-character UTC string,
// so lexicographic comparison of the column is temporal comparison. (With
// plain RFC3339Nano text, "…00.1Z" would sort before "…00Z" because '.'
// precedes 'Z', inverting the real order of mixed-precision instants.)
const sortKeyLayout = "2006-01-02T15:04:05.000000000Z"

// sortKey renders an instant in the stored canonical form.
func sortKey(t time.Time) string {
	return t.UTC().Format(sortKeyLayout)
}

// canonTS converts any RFC3339 timestamp text — a client-supplied bound, a
// keyset position carried by an older cursor, or an already-canonical
// stored key — into the stored sortable form. The conversion is idempotent.
func canonTS(s string) (string, error) {
	if s == "" {
		return "", nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return "", fmt.Errorf("timestamp %q is not valid RFC3339: %w", s, err)
	}
	return sortKey(t), nil
}

// display renders a stored sortable key in the canonical RFC3339Nano UTC
// form the API has always returned (trailing fractional zeros stripped).
func display(key string) string {
	t, err := time.Parse(time.RFC3339Nano, key)
	if err != nil {
		return key
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// ConflictError reports a rejected batch. Existing is true when a sampleId
// was already present in the stream; otherwise the batch itself contained
// duplicate ids.
type ConflictError struct {
	SampleID string
	Existing bool
}

func (e *ConflictError) Error() string {
	if e.Existing {
		return "sampleId already exists in stream: " + e.SampleID
	}
	return "duplicate sampleId within batch: " + e.SampleID
}

// Store wraps the SQLite database.
type Store struct {
	db *sql.DB
	// wmu funnels all write transactions through one goroutine. SQLite is
	// locked per database anyway; serializing also makes batch validation
	// and insert against the live table race-free.
	wmu chan struct{}
}

// Open opens (creating if needed) the database at dsn and applies the
// schema and pragmatic pragmas for a small single-node service.
func Open(ctx context.Context, dsn string) (*Store, error) {
	// _txlock=immediate makes write transactions acquire the write lock
	// up front, avoiding deadlock retries between concurrent writers.
	if !strings.Contains(dsn, "?") {
		dsn += "?"
	} else {
		dsn += "&"
	}
	dsn += "_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)&_txlock=immediate"

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(0)

	s := &Store{db: db, wmu: make(chan struct{}, 1)}
	s.wmu <- struct{}{} // token initially available

	if err := s.init(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := s.migrateSortKeys(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("timestamp migration: %w", err)
	}
	return s, nil
}

func (s *Store) init(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS samples (
			stream_id  TEXT NOT NULL,
			sample_id  TEXT NOT NULL,
			ts         TEXT NOT NULL,
			value      INTEGER NOT NULL,
			seq        INTEGER NOT NULL,
			PRIMARY KEY (stream_id, sample_id)
		) WITHOUT ROWID`,
		`CREATE INDEX IF NOT EXISTS idx_samples_walk
			ON samples (stream_id, ts, sample_id, seq)`,
		`CREATE TABLE IF NOT EXISTS stream_meta (
			stream_id TEXT PRIMARY KEY,
			next_seq  INTEGER NOT NULL
		) WITHOUT ROWID`,
	}
	for _, q := range stmts {
		if _, err := s.db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("schema init: %w", err)
		}
	}
	return nil
}

// Close releases the database handles.
func (s *Store) Close() error { return s.db.Close() }

// migrateSortKeys rewrites ts values stored by older versions — plain
// RFC3339Nano text, whose lexicographic order is not temporal order once
// fractional-second precision varies — to the canonical sortable form. It
// is idempotent and a no-op on databases written by the current version.
func (s *Store) migrateSortKeys(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `SELECT stream_id, sample_id, ts FROM samples`)
	if err != nil {
		return err
	}
	type fix struct {
		streamID, sampleID, key string
	}
	var fixes []fix
	for rows.Next() {
		var f fix
		var ts string
		if err := rows.Scan(&f.streamID, &f.sampleID, &ts); err != nil {
			rows.Close()
			return err
		}
		t, err := time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			continue // not a form this service ever wrote; leave untouched
		}
		if key := sortKey(t); key != ts {
			f.key = key
			fixes = append(fixes, f)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if len(fixes) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx,
		`UPDATE samples SET ts = ? WHERE stream_id = ? AND sample_id = ?`)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	for _, f := range fixes {
		if _, err := stmt.ExecContext(ctx, f.key, f.streamID, f.sampleID); err != nil {
			_ = stmt.Close()
			_ = tx.Rollback()
			return err
		}
	}
	if err := stmt.Close(); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// Ping verifies the database is reachable.
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// CurrentSeq returns the highest sequence assigned in the stream. A stream
// that has never received a post reports 0.
func (s *Store) CurrentSeq(ctx context.Context, streamID string) (int64, error) {
	var next int64
	err := s.db.QueryRowContext(ctx,
		`SELECT next_seq FROM stream_meta WHERE stream_id = ?`, streamID).Scan(&next)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return next, nil
}

// Input is a sample as posted by a client.
type Input struct {
	SampleID string
	TS       time.Time
	Value    int64
}

// InsertBatch validates and inserts between 1 and 100 samples atomically.
// On any intra-batch duplicate or conflict with an existing sampleId the
// whole batch is rejected and nothing is written. It returns the new
// current sequence of the stream after the commit.
func (s *Store) InsertBatch(ctx context.Context, streamID string, in []Input) (newSeq int64, err error) {
	if len(in) == 0 || len(in) > 100 {
		return 0, fmt.Errorf("batch size must be between 1 and 100, got %d", len(in))
	}

	// Fail fast on duplicates inside the batch before touching the DB.
	seen := make(map[string]struct{}, len(in))
	for _, x := range in {
		if _, dup := seen[x.SampleID]; dup {
			return 0, &ConflictError{SampleID: x.SampleID, Existing: false}
		}
		seen[x.SampleID] = struct{}{}
	}

	token := <-s.wmu
	defer func() { s.wmu <- token }()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	var next int64
	row := tx.QueryRowContext(ctx,
		`SELECT next_seq FROM stream_meta WHERE stream_id = ?`, streamID)
	switch err = row.Scan(&next); {
	case errors.Is(err, sql.ErrNoRows):
		next = 0
	case err != nil:
		return 0, err
	}

	// Re-check duplicates against live data in the same transaction.
	ids := make([]string, len(in))
	for i, x := range in {
		ids[i] = x.SampleID
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	q := `SELECT sample_id FROM samples WHERE stream_id = ? AND sample_id IN (` + placeholders + `)`
	args := make([]any, 0, len(ids)+1)
	args = append(args, streamID)
	for _, id := range ids {
		args = append(args, id)
	}
	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		return 0, err
	}
	var conflict string
	for rows.Next() {
		if err := rows.Scan(&conflict); err != nil {
			rows.Close()
			return 0, err
		}
		rows.Close()
		return 0, &ConflictError{SampleID: conflict, Existing: true}
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return 0, err
	}

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO samples (stream_id, sample_id, ts, value, seq) VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	for _, x := range in {
		next++
		if _, err = stmt.ExecContext(ctx, streamID, x.SampleID,
			sortKey(x.TS), x.Value, next); err != nil {
			return 0, err
		}
	}

	if _, err = tx.ExecContext(ctx,
		`INSERT INTO stream_meta(stream_id, next_seq) VALUES(?, ?)
		 ON CONFLICT(stream_id) DO UPDATE SET next_seq = excluded.next_seq`,
		streamID, next); err != nil {
		return 0, err
	}

	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return next, nil
}

// Page is one slice of a snapshot walk.
type Page struct {
	Items []Sample
	// NextAfterTS/NextAfterID are the opaque keyset position to continue
	// from; feed them back as PageOptions.AfterTS/AfterID. Empty means the
	// walk is done.
	NextAfterTS string
	NextAfterID string
}

// PageOptions narrows a snapshot walk.
type PageOptions struct {
	StreamID string
	// From/To accept any RFC3339 form; "" means unbounded. To is exclusive.
	From, To string
	SnapSeq  int64
	// AfterTS/AfterID continue strictly after this keyset position;
	// AfterTS "" = start. AfterTS may be any RFC3339 form, including
	// positions issued by earlier versions of the service.
	AfterTS string
	AfterID string
	Limit   int
}

// FetchPage returns the next limit samples of the immutable snapshot
// (stream, range, seq <= snapSeq) strictly after the keyset position,
// ordered by (timestamp, sampleId).
func (s *Store) FetchPage(ctx context.Context, o PageOptions) (Page, error) {
	// Bounds and the keyset position may arrive as any RFC3339 text (query
	// bounds normalized by the API, positions carried by older cursors);
	// canonicalize so the textual comparison in SQL is a temporal one.
	from, err := canonTS(o.From)
	if err != nil {
		return Page{}, err
	}
	to, err := canonTS(o.To)
	if err != nil {
		return Page{}, err
	}
	after, err := canonTS(o.AfterTS)
	if err != nil {
		return Page{}, err
	}

	var (
		where []string
		args  []any
	)
	where = append(where, "stream_id = ?", "seq <= ?")
	args = append(args, o.StreamID, o.SnapSeq)
	if from != "" {
		where = append(where, "ts >= ?")
		args = append(args, from)
	}
	if to != "" {
		where = append(where, "ts < ?")
		args = append(args, to)
	}
	if after != "" {
		where = append(where, "(ts > ? OR (ts = ? AND sample_id > ?))")
		args = append(args, after, after, o.AfterID)
	}

	q := `SELECT stream_id, sample_id, ts, value, seq FROM samples
	      WHERE ` + strings.Join(where, " AND ") + `
	      ORDER BY ts ASC, sample_id ASC
	      LIMIT ?`
	args = append(args, o.Limit+1)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return Page{}, err
	}
	defer rows.Close()

	page := Page{Items: make([]Sample, 0, o.Limit)}
	for rows.Next() {
		var sm Sample
		if err := rows.Scan(&sm.StreamID, &sm.SampleID, &sm.TS, &sm.Value, &sm.Seq); err != nil {
			return Page{}, err
		}
		page.Items = append(page.Items, sm)
	}
	if err := rows.Err(); err != nil {
		return Page{}, err
	}

	if len(page.Items) > o.Limit {
		last := page.Items[o.Limit-1]
		page.NextAfterTS = last.TS
		page.NextAfterID = last.SampleID
		page.Items = page.Items[:o.Limit]
	}
	// Rows carry the internal sortable key; callers get the canonical
	// RFC3339Nano form the service has always returned.
	for i := range page.Items {
		page.Items[i].TS = display(page.Items[i].TS)
	}
	return page, nil
}

// CountSnapshot counts items belonging to a snapshot definition. It is used
// by tests/verification to assert that a full walk returns every item of
// the snapshot exactly once.
func (s *Store) CountSnapshot(ctx context.Context, streamID, from, to string, snapSeq int64) (int, error) {
	fromKey, err := canonTS(from)
	if err != nil {
		return 0, err
	}
	toKey, err := canonTS(to)
	if err != nil {
		return 0, err
	}
	var where []string
	var args []any
	where = append(where, "stream_id = ?", "seq <= ?")
	args = append(args, streamID, snapSeq)
	if fromKey != "" {
		where = append(where, "ts >= ?")
		args = append(args, fromKey)
	}
	if toKey != "" {
		where = append(where, "ts < ?")
		args = append(args, toKey)
	}
	var n int
	err = s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM samples WHERE `+strings.Join(where, " AND "),
		args...).Scan(&n)
	return n, err
}
