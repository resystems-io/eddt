package hash

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"testing"
)

// go test -bench=.
// versus
// GODEBUG=cpu.all=off go test -bench=.

func BenchmarkSHA1(b *testing.B) {

	// Test with different buffer sizes
	type bench_size = struct {
		name string
		size int
	}
	sizes := []bench_size{
		{"small-100B", 100},
		{"medium-1MiB", 1024 * 1024},
		{"large-½GiB", 500 * 1024 * 1024},
	}

			bootstrap := func(b *testing.B, s bench_size) []byte {
				// Create a buffer of a fixed size (e.g., 1MB).
				buf := make([]byte, s.size)
				// Fill the buffer with random data.
				if _, err := rand.Read(buf); err != nil {
					b.Fatal(err)
				}
				b.SetBytes(int64(len(buf)))
				b.ResetTimer()

				return buf
			}

	// SHA1 only
	for _, s := range sizes {
		b.Run(fmt.Sprintf("sha1-only/%s", s.name), func(b *testing.B) {

			buf := bootstrap(b, s)

			// for i := 0; i < b.N; i++ {
			for b.Loop() {
				// Run the SHA1 hash function on the buffer.
				sum := sha1.Sum(buf)
				if len(sum) != 20 {
					b.Error("sum length incorrect")
				}
			}
		})
	}

	// SHA1 with encoding to hex string
	for _, s := range sizes {
		b.Run(fmt.Sprintf("sha1-hexenc/%s", s.name), func(b *testing.B) {
			buf := bootstrap(b, s)

			// for i := 0; i < b.N; i++ {
			for b.Loop() {
				// Run the SHA1 hash function on the buffer.
				sum := sha1.Sum(buf)
				if len(sum) != 20 {
					b.Error("sum length incorrect")
				}
				sha1String := hex.EncodeToString(sum[:])
				expect := 40
				if len(sha1String) != expect {
					b.Errorf("sum string length incorrect %d != %d", expect, len(sha1String))
				}
			}
		})
	}

}
