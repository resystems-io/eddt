package token

import (
	"strings"
	"testing"
)

func BenchmarkSplit(b *testing.B) {

	b.Run("unbounded", func(b *testing.B) {
		for b.Loop() {
			_ = strings.Split("one.two.three.four.five", ".")
		}
	})
	b.Run("bounded", func(b *testing.B) {
		for b.Loop() {
			_ = strings.SplitN("one.two.three.four.five", ".", 5)
		}
	})
}
