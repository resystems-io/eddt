package metrics

import (
	"fmt"
	"time"
)

// Example 1: Standard CPU usage
func ExampleInfluxLinePoint_cpu() {

	point1 := InfluxLinePoint{
		Measurement: "cpu",
		Tags: map[string]string{
			"host":   "serverA",
			"region": "us-west",
		},
		Fields: map[string]any{
			"usage_idle": 85.1,
			"usage_user": 12.3,
		},
		Timestamp: time.Date(2025, 10, 31, 10, 0, 0, 0, time.UTC),
	}
	lp1, err := point1.ToLineProtocol()
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Println("Point 1:", lp1)
	}

	// Output:
	// Point 1: cpu,host=serverA,region=us-west usage_idle=85.1,usage_user=12.3 1761904800000000000
}

// Example 2: With special characters and different field types
func ExampleInfluxLinePoint_mixed() {
	point2 := InfluxLinePoint{
		Measurement: "disk usage", // Contains space
		Tags: map[string]string{
			"device":      "/dev/sda1",
			"data center": "europe-east-1", // Contains space in tag value
		},
		Fields: map[string]any{
			"free_gb":  uint64(250),
			"offset":  int64(-33),
			"used_pct": 75.5,
			"blocks": 75.5e+78,
			"online":   true,
			"message":  "disk \"full\" warning!", // String with quote
		},
		Timestamp: time.Date(2023, 3, 15, 10, 0, 0, 0, time.UTC),
	}
	lp2, err := point2.ToLineProtocol()
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Println("Point 2:", lp2)
	}

	// Output:
	// Point 2: disk\ usage,data\ center=europe-east-1,device=/dev/sda1 blocks=7.55e+79,free_gb=250u,message="disk \"full\" warning!",offset=-33i,online=true,used_pct=75.5 1678874400000000000
}

func ExampleInfluxLinePoint_missing() {
	// Example 3: Missing required field
	point3 := InfluxLinePoint{
		Measurement: "no_fields",
		Tags: map[string]string{
			"test": "fail",
		},
		Timestamp: time.Now(),
	}
	lp3, err := point3.ToLineProtocol()
	if err != nil {
		fmt.Println("Error for point 3 (expected):", err)
	} else {
		fmt.Println("Point 3:", lp3)
	}

	// Output:
	// Error for point 3 (expected): at least one field is required
}
