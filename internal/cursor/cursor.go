// Package cursor implements the opaque, tamper-evident pagination cursor.
//
// The cursor is a self-contained snapshot session: it records the stream,
// the fixed time range, the snapshot upper bound (an ingestion sequence
// number) and the last returned keyset position. It is serialized as JSON,
// wrapped in an HMAC-SHA256 tag so that tampering, cross-stream reuse and
// range changes can be rejected as client errors.
package cursor

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Version is the cursor payload format version.
const Version = 1

// ErrInvalid is returned for any cursor that is malformed, tampered with,
// signed with another secret or otherwise unusable.
var ErrInvalid = errors.New("invalid cursor")

// Token is the signed pagination state carried between page requests.
type Token struct {
	Version int    `json:"v"`
	Stream  string `json:"sid"`
	// From/To are the normalized RFC3339Nano UTC bounds of the session.
	// Empty string means the bound is unbounded.
	From    string `json:"lb,omitempty"`
	To      string `json:"ub,omitempty"`
	SnapSeq int64  `json:"sn"`
	// AfterTS/AfterID are the keyset (timestamp, sampleId) position; empty
	// means the first page.
	AfterTS string `json:"ats,omitempty"`
	AfterID string `json:"aid,omitempty"`
}

// Normalize renders a time instant in the canonical form used both inside
// cursors and in the database, so textual comparison equals temporal order.
func Normalize(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func mustJSON(t Token) []byte {
	raw, err := json.Marshal(t)
	if err != nil {
		panic(err)
	}
	return raw
}

// Sign produces the opaque wire form: base64url(payload).base64url(hmac).
func Sign(t Token, secret []byte) string {
	t.Version = Version
	payload := base64.RawURLEncoding.EncodeToString(mustJSON(t))
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(payload))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return payload + "." + sig
}

// Parse validates the signature and shape of a cursor and returns it.
func Parse(encoded string, secret []byte) (Token, error) {
	dot := strings.LastIndex(encoded, ".")
	if dot <= 0 || dot == len(encoded)-1 {
		return Token{}, ErrInvalid
	}
	payload, sig := encoded[:dot], encoded[dot+1:]

	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(payload))
	want := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if subtle.ConstantTimeCompare([]byte(sig), []byte(want)) != 1 {
		return Token{}, ErrInvalid
	}

	raw, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return Token{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	var t Token
	if err := json.Unmarshal(raw, &t); err != nil {
		return Token{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if t.Version != Version || t.Stream == "" || t.SnapSeq <= 0 {
		return Token{}, ErrInvalid
	}
	if (t.AfterTS == "") != (t.AfterID == "") {
		return Token{}, ErrInvalid
	}
	return t, nil
}
