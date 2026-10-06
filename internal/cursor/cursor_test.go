package cursor

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"
)

func TestSignParseRoundtrip(t *testing.T) {
	secret := []byte("super-secret")
	tok := Token{
		Stream:  "buoy-7",
		From:    Normalize(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
		To:      "",
		SnapSeq: 42,
		AfterTS: "2026-01-01T00:00:00Z",
		AfterID: "abc",
	}
	enc := Sign(tok, secret)
	if strings.Contains(enc, " ") {
		t.Fatalf("cursor must be a single opaque token, got %q", enc)
	}
	got, err := Parse(enc, secret)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := tok
	want.Version = Version // Sign stamps the current version
	if got != want {
		t.Fatalf("roundtrip mismatch:\n got=%+v\nwant=%+v", got, want)
	}
	if got.Version != Version {
		t.Fatalf("version not stamped: %d", got.Version)
	}
}

func TestParseRejectsTampering(t *testing.T) {
	secret := []byte("super-secret")
	enc := Sign(Token{Stream: "s", SnapSeq: 3}, secret)

	// Flip a character in the signature.
	tampered := enc
	if strings.HasSuffix(tampered, "A") {
		tampered = strings.TrimSuffix(tampered, "A") + "B"
	} else {
		tampered = strings.TrimSuffix(tampered, tampered[len(tampered)-1:]) + "A"
	}
	if _, err := Parse(tampered, secret); err != ErrInvalid {
		t.Fatalf("tampered signature: want ErrInvalid, got %v", err)
	}

	// Flip payload bits without updating the tag.
	dot := strings.Index(enc, ".")
	payload := enc[:dot]
	raw, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0x01
	rebuilt := base64.RawURLEncoding.EncodeToString(raw) + enc[dot:]
	if _, err := Parse(rebuilt, secret); err != ErrInvalid {
		t.Fatalf("tampered payload: want ErrInvalid, got %v", err)
	}
}

func TestParseRejectsWrongSecretAndGarbage(t *testing.T) {
	enc := Sign(Token{Stream: "s", SnapSeq: 3}, []byte("secret-a"))
	if _, err := Parse(enc, []byte("secret-b")); err != ErrInvalid {
		t.Fatalf("wrong secret: want ErrInvalid, got %v", err)
	}
	for _, garbage := range []string{"", "nodot", ".", "abc.", "!!!.!!!", "a.b.c"} {
		if _, err := Parse(garbage, []byte("k")); err != ErrInvalid {
			t.Fatalf("garbage %q: want ErrInvalid, got %v", garbage, err)
		}
	}
}

func TestParseRejectsMalformedPayload(t *testing.T) {
	secret := []byte("k")
	for _, tok := range []Token{
		{Stream: "", SnapSeq: 1},                // missing stream
		{Stream: "s", SnapSeq: 0},               // missing snapshot
		{Stream: "s", SnapSeq: 1, AfterTS: "x"}, // half keyset
	} {
		if _, err := Parse(Sign(tok, secret), secret); err != ErrInvalid {
			t.Fatalf("token %+v: want ErrInvalid, got %v", tok, err)
		}
	}
}
