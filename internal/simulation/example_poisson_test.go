package simulation

import (
	"fmt"
	"math/rand/v2"

	"gonum.org/v1/gonum/stat/distuv"
)

func ExampleWallClock() {

	// Po(X = λ) = \frac{λ^y·e^{-y}}{y!}
	// With the expected value E(y) having a mean and variance of λ i.e. µ = σ² = λ

	poisson := distuv.Poisson{
		Lambda: 5.0,
		Src:    rand.NewPCG(1, 1),
	}

	fmt.Println("Generating 10 random numbers from a Poisson distribution (Lambda = 5.0):")

	for i := range 10 {
		// The Rand() method returns a random sample from the distribution.
		sample := poisson.Rand()
		fmt.Printf("Sample #%d: %d\n", i+1, int(sample))
	}

	// Output:
	// Generating 10 random numbers from a Poisson distribution (Lambda = 5.0):
	// Sample #1: 4
	// Sample #2: 4
	// Sample #3: 7
	// Sample #4: 1
	// Sample #5: 7
	// Sample #6: 7
	// Sample #7: 7
	// Sample #8: 3
	// Sample #9: 2
	// Sample #10: 1
}
