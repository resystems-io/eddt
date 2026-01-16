package metrics

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// InfluxLinePoint represents a single data point for InfluxDB Line Protocol
type InfluxLinePoint struct {
	Measurement string
	Tags        map[string]string
	Fields      map[string]any // Values can be int64, uint64, float64, bool, string
	Timestamp   time.Time
}

// EscapeMeasurementTagFieldKey escapes characters for measurement, tag keys, field keys
func EscapeMeasurementTagFieldKey(s string) string {
	s = strings.ReplaceAll(s, " ", "\\ ")
	s = strings.ReplaceAll(s, ",", "\\,")
	s = strings.ReplaceAll(s, "=", "\\=")
	return s
}

// EscapeTagValue escapes characters for tag values
func EscapeTagValue(s string) string {
	s = strings.ReplaceAll(s, " ", "\\ ")
	s = strings.ReplaceAll(s, ",", "\\,")
	s = strings.ReplaceAll(s, "=", "\\=")
	return s
}

// EscapeStringFieldValue escapes characters for string field values
func EscapeStringFieldValue(s string) string {
	s = strings.ReplaceAll(s, "\"", "\\\"")
	return fmt.Sprintf("\"%s\"", s)
}

// ToLineProtocol converts the InfluxLinePoint to a line protocol string.
//
// See: https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/
func (p *InfluxLinePoint) ToLineProtocol() (string, error) {
	if p.Measurement == "" {
		return "", fmt.Errorf("measurement cannot be empty")
	}
	if len(p.Fields) == 0 {
		return "", fmt.Errorf("at least one field is required")
	}

	var sb strings.Builder

	// 1. Measurement
	sb.WriteString(EscapeMeasurementTagFieldKey(p.Measurement))

	// 2. Tags (sorted for consistent output)
	if len(p.Tags) > 0 {
		keys := make([]string, 0, len(p.Tags))
		for k := range p.Tags {
			keys = append(keys, k)
		}
		sort.Strings(keys) // Important for consistent line protocol if order matters for ingestion/deduplication

		for _, k := range keys {
			sb.WriteString(",")
			sb.WriteString(EscapeMeasurementTagFieldKey(k)) // Tag Key
			sb.WriteString("=")
			sb.WriteString(EscapeTagValue(p.Tags[k])) // Tag Value
		}
	}

	// 3. Fields (sorted for consistent output)
	sb.WriteString(" ")
	fieldKeys := make([]string, 0, len(p.Fields))
	for k := range p.Fields {
		fieldKeys = append(fieldKeys, k)
	}
	sort.Strings(fieldKeys) // Important for consistent line protocol

	firstField := true
	for _, k := range fieldKeys {
		if !firstField {
			sb.WriteString(",")
		}
		sb.WriteString(EscapeMeasurementTagFieldKey(k)) // Field Key
		sb.WriteString("=")

		v := p.Fields[k]
		switch val := v.(type) {
		case int, int8, int16, int32, int64:
			sb.WriteString(fmt.Sprintf("%di", val)) // InfluxDB integers require 'i' suffix
		case uint, uint8, uint16, uint32, uint64:
			sb.WriteString(fmt.Sprintf("%du", val)) // InfluxDB unsigned integers require 'u' suffix
		case float32:
			sb.WriteString(strconv.FormatFloat(float64(val), 'g', -1, 32)) // No 'f' suffix for floats
		case float64:
			sb.WriteString(strconv.FormatFloat(val, 'g', -1, 64))
		case bool:
			sb.WriteString(strconv.FormatBool(val))
		case string:
			sb.WriteString(EscapeStringFieldValue(val))
		default:
			return "", fmt.Errorf("unsupported field value type for key '%s': %T", k, val)
		}
		firstField = false
	}

	// 4. Timestamp
	if !p.Timestamp.IsZero() {
		sb.WriteString(" ")
		sb.WriteString(strconv.FormatInt(p.Timestamp.UnixNano(), 10))
	}

	return sb.String(), nil
}
