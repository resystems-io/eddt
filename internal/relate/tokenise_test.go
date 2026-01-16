package relate

import (
	"fmt"
	"strings"
)

func ExampleCompilerRule() {
	example := "one.two.three.four.five.and-more"
	parts_inner := strings.SplitN(example, ".", 4)
	parts_edge := strings.SplitN(example, ".", 6)
	parts_outer := strings.SplitN(example, ".", 10)

	fmt.Printf("%d %v\n", len(parts_inner), parts_inner)
	fmt.Printf("%d %v\n", len(parts_edge), parts_edge)
	fmt.Printf("%d %v\n", len(parts_outer), parts_outer)

	// Output:
	// 4 [one two three four.five.and-more]
    // 6 [one two three four five and-more]
	// 6 [one two three four five and-more]
}
