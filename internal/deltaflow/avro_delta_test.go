package deltaflow

import (
	"bytes"
	_ "embed"
	"os"
	"path/filepath"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ConfigTeardown() {
	// Reset the caches
	avro.DefaultConfig = avro.Config{}.Freeze()
}

func Test_avro_delta(t *testing.T) {

	// Note, hamba/avro requires all schemas to be parsed, but it adds them to its internal cache.
	// Note, unions with enums require registring of the enum Go type with the appropriate namespace.

	schema_fixtures := func(verbose bool) (avro.Schema, avro.Schema, avro.Schema, avro.Schema) {

		// Attempting to include "aliases", or "namespace".
		// Note, adding aliases did not seem to help with the resolving of types.
		avro.WithAliases([]string{"deltaflow"})

		schema_delta, err := avro.Parse(string(avro_delta_schema))
		if err != nil {
			t.Fatal(err)
		}
		schema_geo, err := avro.Parse(string(avro_geo_schema))
		if err != nil {
			t.Fatal(err)
		}
		schema_scooter, err := avro.Parse(string(avro_scooter_schema))
		if err != nil {
			t.Fatal(err)
		}
		schema, err := avro.Parse(schema_scooter.String())
		if err != nil {
			t.Fatal(err)
		}
		if verbose {
			t.Logf("schema delta: %v", schema_delta.String())
			t.Logf("schema geo: %v", schema_geo.String())
			t.Logf("schema scooter: %v", schema_scooter.String())
			t.Logf("========================")
			t.Logf("schema fingerprint: %v", schema.CacheFingerprint())
			t.Logf("schema type: %v", schema.Type())
			t.Logf("schema all: %v", schema.String())
			t.Logf("schema type: %v", schema.Type())
			t.Logf("schema all: %v", schema.String())
		}

		return schema_delta, schema_geo, schema_scooter, schema
	}

	// A potential query...
	q := `SELECT
    -- Check which branch of the union is active (0=float, 1=DeltaOp)
    pitch.tag,

    -- Extract the value if tag=0
    pitch.float AS pitch_value,

    -- Extract the op if tag=1
    pitch.DeltaOp AS pitch_op
	FROM read_avro('updates.avro');
	`

	t.Log(q)
	t.Run("point", func(t *testing.T) {
		defer ConfigTeardown()
		t.Run("ocf", func(t *testing.T) {

			_, schema_geo, _, _ := schema_fixtures(false)

			type new_point func(lat, lon float64) any

			new_struct_point := func(lat, lon float64) any {
				return &GeoPoint{
					Lat: lat,
					Lon: lon,
				}
			}

			new_map_point := func(lat, lon float64) any {
				return map[string]any{
					"lat": lat,
					"lon": lon,
				}
			}

			type point_case struct {
				name      string
				new_point new_point
			}

			alt := []point_case{
				{name: "struct-based", new_point: new_struct_point},
				{name: "map-based", new_point: new_map_point},
			}

			for _, c := range alt {
				t.Run(c.name, func(t *testing.T) {
					// Create a new OCF file
					tmpDir := t.TempDir()
					delta_avro_path := filepath.Join(tmpDir, "delta.avro")
					f, err := os.Create(delta_avro_path)
					if err != nil {
						t.Fatal(err)
					}
					defer os.Remove(delta_avro_path)

					// Create a new encoder (using the string schema, rather than a pre-parsed schema)
					enc, err := ocf.NewEncoderWithSchema(schema_geo, f)
					if err != nil {
						t.Fatal(err)
					}

					// Write some records
					for i := 0; i < 3; i++ {
						update := c.new_point(27.123, 45.678+float64(i))

						if err := enc.Encode(update); err != nil {
							t.Fatal(err)
						}
					}

					if err := enc.Close(); err != nil {
						t.Fatal(err)
					}
					f.Close()

					// Read it back to verify
					f, err = os.Open(delta_avro_path)
					if err != nil {
						t.Fatal(err)
					}
					defer f.Close()

					dec, err := ocf.NewDecoder(f)
					if err != nil {
						t.Fatal(err)
					}

					for dec.HasNext() {
						var out GeoPoint
						if err := dec.Decode(&out); err != nil {
							t.Fatal(err)
						}
						t.Log(out)
						if out.Lat != 27.123 {
							t.Errorf("Expected Lat to be 27.123, got %v", out.Lat)
						}
						if out.Lon < 45.0 && out.Lon > 48.0 {
							t.Errorf("Expected Lon to be in the range [45.0, 48.0], got %v", out.Lon)
						}
					}
				})
			}
		})
	})

	t.Run("update", func(t *testing.T) {

		t.Run("struct-based", func(t *testing.T) {
			defer ConfigTeardown()

			_, _, schema_scooter, _ := schema_fixtures(false)

			// Note, the namespace needs name in the schema.
			deltaOpType := "io.resystems.eddt.types.DeltaOp"
			avro.DefaultConfig.Register(deltaOpType, DeltaOpIgnore)
			geoPointType := "io.resystems.eddt.types.GeoPoint"
			avro.DefaultConfig.Register(geoPointType, &GeoPoint{})
			registeredType, err := avro.DefaultConfig.TypeOf(deltaOpType)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("registered type: %v (%T)", registeredType, registeredType)
			names, err := avro.DefaultConfig.NamesOf(registeredType)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("names: %v", names)

			update_record := NewScooterUpdate()
			update_record.SpeedometerSpeed = float32(25.5)
			update_record.MotorTemperature = DeltaOpReset
			update_record.Location = &GeoPoint{
				Lat: 27.123,
				Lon: 45.678,
			}

			data, err := avro.Marshal(schema_scooter, update_record)
			if err != nil {
				t.Fatal(err)
			}
			t.Log(data)

			// Verify the data
			var out ScooterUpdate
			if err := avro.Unmarshal(schema_scooter, data, &out); err != nil {
				t.Fatal(err)
			}
			t.Logf("out.ActivityState: %v (%T)", out.ActivityState, out.ActivityState)
			t.Logf("out.SpeedometerSpeed: %v (%T)", out.SpeedometerSpeed, out.SpeedometerSpeed)
			t.Logf("out.MotorTemperature: %v (%T)", out.MotorTemperature, out.MotorTemperature)
			t.Logf("out.MotorSpeed: %v (%T)", out.MotorSpeed, out.MotorSpeed)
			t.Logf("out.Location: %v (%T)", out.Location, out.Location)
			assert.Equal(t, DeltaOpIgnore, out.ActivityState)
			assert.Equal(t, float32(25.5), out.SpeedometerSpeed)
			assert.Equal(t, DeltaOpReset, out.MotorTemperature)
			t.Log(out)

		})
		t.Run("map-based", func(t *testing.T) {
			defer ConfigTeardown()
			avro.DefaultConfig.Register("io.resystems.eddt.types.DeltaOp", DeltaOpIgnore)

			schema := `{
				"type": "record",
				"name": "ScooterUpdate",
				"fields": [
					{
						"name": "activity_state",
						"type": [
							{
								"type":"enum",
								"aliases":["deltaflow.DeltaOp"],
								"namespace":"io.resystems.eddt.types",
								"name": "DeltaOp",
								"symbols": ["IGNORE", "RESET"]
							}
							, "string"
							, "null"
						],
						"default": "IGNORE"
					},
					{
						"name": "speedometer_speed",
						"type": [
							"io.resystems.eddt.types.DeltaOp",
							"float",
							"null"
						],
						"default": "IGNORE"
					},
					{
						"name": "motor_temperature",
						"type": [
							"io.resystems.eddt.types.DeltaOp",
							"float",
							"null"
						],
						"default": "IGNORE"
					}
				]
			}`

			buf := bytes.NewBuffer([]byte{})
			enc, err := avro.NewEncoder(schema, buf)
			if err != nil {
				t.Fatal(err)
			}

			update_record := map[string]any{
				"activity_state":    DeltaOpIgnore,
				"speedometer_speed": float32(25.5),
				"motor_temperature": DeltaOpReset,
			}

			err = enc.Encode(update_record)
			if err != nil {
				t.Fatal(err)
			}

			t.Log(buf.Bytes())
		})
	})

}

func Test_avro_enum_simple(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", {"type":"enum", "name": "test", "symbols": ["foo", "bar"]}]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	if err != nil {
		t.Fatal(err)
	}

	err = enc.Encode(map[string]any{"test": "bar"})

	if err != nil {
		t.Fatal(err)
	}
	if !assert.Equal(t, []byte{0x02, 0x02}, buf.Bytes()) {
		t.Errorf("Expected %v, got %v", []byte{0x02, 0x02}, buf.Bytes())
	}
}

func Test_avro_enum_symbols(t *testing.T) {
	// Test that we can in fact set a field to an enum, and that if fails if the wrong symbol is provided.
	// But, this test does not include the use of unions.

	type updates struct {
		name  string
		err   string
		value any
	}

	cases := []updates{
		{"IGNORE", "", "IGNORE"},
		{"RESET", "", "RESET"},
		{"IGFFFF", "avro: unknown enum symbol: IGFFFF", "IGFFFF"},
		{"DeltaOpReset", "", DeltaOpReset},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer ConfigTeardown()

			schema := `{
						"type": "record",
						"name": "ScooterUpdate",
						"fields": [
							{
								"name": "activity_state",
								"type": {"type":"enum", "aliases": ["deltaflow.DeltaOp"], "name": "testoenum", "symbols": ["IGNORE", "RESET"]}
							}
						]
					}`

			buf := bytes.NewBuffer([]byte{})
			enc, err := avro.NewEncoder(schema, buf)
			if err != nil {
				t.Fatal(err)
			}

			update_record := map[string]any{
				"activity_state": c.value,
			}

			err = enc.Encode(update_record)
			t.Logf("error returned: %v", err)
			if len(c.err) > 0 {
				assert.ErrorContains(t, err, c.err)
			} else {
				require.NoError(t, err)
			}

			t.Log(buf.Bytes())
		})
	}
}
