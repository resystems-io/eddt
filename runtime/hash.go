package runtime

import (
	"encoding/binary"
	"hash"
	"io"

	"golang.org/x/crypto/blake2b"
)

// NewHash returns a new Blake2b-256 hasher for use in EntityID() methods
// emitted by delta-gen. The hash function is fixed forever; changing it
// would invalidate all stored EntityIDs (Errata E-10).
//
// Generated EntityID() methods follow this pattern:
//
//	func (k MyKey) EntityID() runtime.EntityID {
//	    h := runtime.NewHash()
//	    runtime.WriteString(h, k.Field1)
//	    runtime.WriteUint64(h, k.Field2)
//	    return runtime.Finalise(h)
//	}
func NewHash() hash.Hash {
	// blake2b.New256 only errors when given a non-nil key of invalid length.
	// We always pass nil (unkeyed), so the error is unreachable in practice.
	h, _ := blake2b.New256(nil)
	return h
}

// Finalise extracts the Blake2b-256 digest and returns it as an EntityID.
// Call this after all key fields have been written to h.
func Finalise(h hash.Hash) EntityID {
	var id EntityID
	h.Sum(id[:0])
	return id
}

// WriteUint8 appends the single-byte big-endian encoding of v to w.
func WriteUint8(w io.Writer, v uint8) {
	_, _ = w.Write([]byte{v})
}

// WriteUint16 appends the 2-byte big-endian encoding of v to w.
func WriteUint16(w io.Writer, v uint16) {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], v)
	_, _ = w.Write(buf[:])
}

// WriteUint32 appends the 4-byte big-endian encoding of v to w.
func WriteUint32(w io.Writer, v uint32) {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], v)
	_, _ = w.Write(buf[:])
}

// WriteUint64 appends the 8-byte big-endian encoding of v to w.
func WriteUint64(w io.Writer, v uint64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	_, _ = w.Write(buf[:])
}

// WriteBool appends 0x00 for false and 0x01 for true to w.
func WriteBool(w io.Writer, v bool) {
	if v {
		_, _ = w.Write([]byte{0x01})
	} else {
		_, _ = w.Write([]byte{0x00})
	}
}

// WriteString appends a length-prefixed UTF-8 string to w. The 8-byte
// big-endian byte-length of the string precedes its UTF-8 content,
// preventing prefix-collision between strings of different lengths.
func WriteString(w io.Writer, v string) {
	WriteUint64(w, uint64(len(v)))
	_, _ = io.WriteString(w, v)
}

// WriteNilMarker appends a presence byte to w: 0x00 for absent (nil pointer),
// 0x01 for present (non-nil pointer). Always call this before writing a
// pointer-typed key field so that nil and non-nil keys produce distinct hashes.
func WriteNilMarker(w io.Writer, present bool) {
	if present {
		_, _ = w.Write([]byte{0x01})
	} else {
		_, _ = w.Write([]byte{0x00})
	}
}
