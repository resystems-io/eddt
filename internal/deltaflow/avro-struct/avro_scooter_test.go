package avrostruct

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"

	"go.resystems.io/eddt/internal/common"
	"go.resystems.io/eddt/internal/deltaflow"
)

func ConfigTeardown() {
	// Reset the caches
	avro.DefaultConfig = avro.Config{}.Freeze()
}

func schema_fixtures(t *testing.T, verbose bool) (avro.Schema, avro.Schema, avro.Schema, avro.Schema) {

	schema_delta, err := avro.Parse(string(deltaflow.AvroDeltaSchema))
	if err != nil {
		t.Fatal(err)
	}
	schema_geo, err := avro.Parse(string(deltaflow.AvroGeoSchema))
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

func register_types(t *testing.T) {
	// Note, the namespace needs name in the schema.
	deltaOpType := "io.resystems.eddt.types.DeltaOp"
	avro.DefaultConfig.Register(deltaOpType, deltaflow.DeltaOpIgnore)
	geoPointType := "io.resystems.eddt.types.GeoPoint"
	avro.DefaultConfig.Register(geoPointType, GeoPoint{})
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
}

const scooter_pitch_view = `
CREATE OR REPLACE VIEW scooter_pitch AS (

	SELECT
		CAST(id AS UUID) AS id,
		CAST(transaction_id AS UUID) AS t,
		pitch.op AS pop,
		pitch.value AS pval
	FROM read_parquet('/tmp/scooter_struct_updates.convert.parquet')
	ORDER BY t

);
`

const scooter_pitch_rollup_view = `
SELECT
    id,

    -- Pick the pval from the row with the highest 't',
    -- but only consider rows where the command is 'ASSERT'
    arg_max(pval, t) FILTER (WHERE pop = 'ASSERT') AS current_pitch,

    -- Optional: Keep the timestamp of when this specific value was asserted
    max(t) FILTER (WHERE pop = 'ASSERT') AS pitch_updated_at,

    -- Optional: Keep the timestamp of the absolute latest event (even if IGNORE)
    max(t) AS last_event_at

FROM scooter_pitch
GROUP BY id;
`

// Note, because arg_max (like other aggregate functions) ignores NULLs,
// we move the CASE outside of the arg_max call.
const scooter_pitch_rollup_with_retract_view = `
SELECT
    id,

    -- Evaluate the latest operation FIRST.
    -- If it's a RETRACT, explicitly output NULL.
    -- Otherwise, output the actual pval from that latest event.
    CASE
        WHEN arg_max(pop, t) FILTER (WHERE pop IN ('ASSERT', 'RETRACT')) = 'RETRACT'
        THEN NULL
        ELSE arg_max(pval, t) FILTER (WHERE pop IN ('ASSERT', 'RETRACT'))
    END AS current_pitch,

    -- The operation that resulted in the state above
    arg_max(pop, t) FILTER (WHERE pop IN ('ASSERT', 'RETRACT')) AS last_applied_op,

    max(t) AS last_event_at

FROM scooter_pitch
GROUP BY id;
`

// An example of the roll-up view:
// - scooter_pitch_rollup_with_retract_view
// ```
// ┌──────────────────────────────────────┬────────────────────┬─────────────────┬──────────────────────────────────────┐
// │                  id                  │   current_pitch    │ last_applied_op │            last_event_at             │
// │                 uuid                 │       double       │     varchar     │                 uuid                 │
// ├──────────────────────────────────────┼────────────────────┼─────────────────┼──────────────────────────────────────┤
// │ c445b55e-e798-4e25-9902-dbc477b11e45 │               NULL │ RETRACT         │ 019c7bbd-7937-71c1-b581-4e1ecefd3514 │
// │ 8978dc2a-926e-48f1-ade0-9b5d865cf62e │               NULL │ RETRACT         │ 019c7bbd-7937-71c5-b50e-5dcdf2bd6258 │
// │ 424863a7-eab3-4d91-8b99-fc0f225ad2f9 │               NULL │ RETRACT         │ 019c7bbd-7937-71ca-bdef-eb6b7507ee3c │
// │ 112bb1a0-78bb-4763-9ed9-4142f2e36dec │               NULL │ RETRACT         │ 019c7bbd-7937-71cc-ab57-b20bfb762d70 │
// │ fa70591c-ba85-46ff-99be-f758dd205e74 │  0.999991774559021 │ ASSERT          │ 019c7bbd-7937-71e2-ad76-6bc5c732f489 │
// │ b589e6e4-d661-4534-beef-f9c770295935 │               NULL │ RETRACT         │ 019c7bbd-7937-71f0-9736-8e33f8808766 │
// │ 811df8bb-356c-40ff-a6b0-1e689b63674f │               NULL │ RETRACT         │ 019c7bbd-7937-7214-9355-a98c4f38d50c │
// │ 36c39d8a-4619-429a-9cc0-ac40faa7d500 │               NULL │ RETRACT         │ 019c7bbd-7937-71c3-a6c2-3a98d0835ca8 │
// │ cf23ed9b-a8c6-49be-8d43-75c4390112b7 │               NULL │ RETRACT         │ 019c7bbd-7937-71e6-8cb3-4f4db62158aa │
// │ b488a4fc-d628-48e7-b19c-fe367c735d82 │               NULL │ RETRACT         │ 019c7bbd-7937-718e-abfc-5a71179f621a │
// │ 7a5a92c3-c525-457b-90d9-a8a803297fb5 │ 0.9999349117279053 │ ASSERT          │ 019c7bbd-7937-7191-a688-37456deea1a7 │
// │ 11d4be45-f00a-47e2-9a7b-cc66938d5bcd │ 0.9998495578765869 │ ASSERT          │ 019c7bbd-7937-71a9-903e-562296115860 │
// │ aefd8661-032c-480e-8d7b-7f1b6b63cb3a │               NULL │ RETRACT         │ 019c7bbd-7937-71cb-aea0-b07c505874a8 │
// │ c2eb108e-c788-4a42-a9ae-7ce9848a02ed │               NULL │ RETRACT         │ 019c7bbd-7937-71dc-a1de-4d0031db3d94 │
// │ 36697cea-0036-478b-b944-e948ea00c766 │               NULL │ RETRACT         │ 019c7bbd-7937-71df-90ff-c368047f4088 │
// │ 0934c1a1-ee7c-45dd-8dc8-5e6311d58787 │ 0.9998853206634521 │ ASSERT          │ 019c7bbd-7937-7200-bb8a-9213adfe5424 │
// │ 1e67bfdd-9a87-4184-acce-b7a7186552d4 │               NULL │ RETRACT         │ 019c7bbd-7937-718d-bb4a-7e6f19498093 │
// │ 8e0e9ce8-5e2c-4605-819a-9384763376df │  0.999976634979248 │ ASSERT          │ 019c7bbd-7937-7210-bc9e-d8920cec6c09 │
// │ bb0aefee-fe6a-45e8-b744-604efc9a691f │ 0.9998888969421387 │ ASSERT          │ 019c7bbd-7937-7187-ab4c-d3ca658e38f7 │
// │ cb405533-2c8b-4db6-8db4-8057e10c1a92 │  0.999981164932251 │ ASSERT          │ 019c7bbd-7937-719d-ae57-a35edaa24144 │
// │                  ·                   │                 ·  │   ·             │                  ·                   │
// │                  ·                   │                 ·  │   ·             │                  ·                   │
// │                  ·                   │                 ·  │   ·             │                  ·                   │
// │ 5d598a9e-648b-4715-afa2-587aec1ff8fd │               NULL │ RETRACT         │ 019c7bbd-7937-71dd-8b9a-21b84355304e │
// │ 7c1924b1-82b4-4bdf-87cd-16febec0782a │               NULL │ RETRACT         │ 019c7bbd-7937-71a1-93dd-0dbedb1a5dc0 │
// │ d001719b-32c7-46ab-af8f-df1e6e0fd439 │ 0.9999871253967285 │ ASSERT          │ 019c7bbd-7937-71c4-8d71-b0c459d64163 │
// │ a122a5db-e176-4791-b223-3e576aa68d3f │               NULL │ RETRACT         │ 019c7bbd-7937-71d2-a97e-f7e447b94cf7 │
// │ 4627eeca-8f6e-4f6d-b8ff-e5e6478edb51 │               NULL │ RETRACT         │ 019c7bbd-7937-71e1-a969-0e384e3802bf │
// │ 9c2332d3-21cc-410c-9c49-89b4d9d87eab │ 0.9999279975891113 │ ASSERT          │ 019c7bbd-7937-71f4-98d8-8759b3a04be5 │
// │ 420fe350-b2ba-4304-b4c8-69a52e08af98 │ 0.9999959468841553 │ ASSERT          │ 019c7bbd-7937-71fd-845e-d93433e6d517 │
// │ d2ff78f8-0fbb-4bc3-9ae1-ed7ea747f9f9 │               NULL │ RETRACT         │ 019c7bbd-7937-71ab-abeb-98db84460ff6 │
// │ e1f36465-af64-47e0-9e5a-25a9d5ae2c01 │ 0.9999853372573853 │ ASSERT          │ 019c7bbd-7937-71b9-8a54-350b0f0fc3a1 │
// │ 1b96cd7e-b1b3-4e58-9618-43d2baa6c3c8 │ 0.9999877214431763 │ ASSERT          │ 019c7bbd-7937-71c8-ad3f-7c549466130e │
// │ d588516f-9fd0-461f-973f-fff122bce340 │               NULL │ RETRACT         │ 019c7bbd-7937-71d1-af29-27f8723e3637 │
// │ 46efe626-c070-484c-a1cd-69f7faf817a7 │  0.999992847442627 │ ASSERT          │ 019c7bbd-7937-71e9-bd16-aea8bf13e686 │
// │ 5903a8f5-0eec-4f3d-8b17-edc15314fc73 │ 0.9999935626983643 │ ASSERT          │ 019c7bbd-7937-71ed-884a-159d3b37df16 │
// │ 69fd91ea-eeda-4ff9-90d0-3336202d35b1 │               NULL │ RETRACT         │ 019c7bbd-7937-71b0-99fd-f29a6da2f2ed │
// │ 88076dba-8c7e-4050-9f8b-3baa24d161cb │ 0.9999442100524902 │ ASSERT          │ 019c7bbd-7937-71ce-857b-fd9150f8337f │
// │ fbaf0dae-cc0e-46d5-8e59-9b92641d42cc │               NULL │ RETRACT         │ 019c7bbd-7937-71d4-aec6-e7a3b43aaf7a │
// │ 6c0b52d9-74da-47cc-b34f-4ff4cc101aa1 │               NULL │ RETRACT         │ 019c7bbd-7937-71f9-b15f-17e01c4611ac │
// │ 26179f54-d12f-4d3b-8f59-57bcea6f8f8e │               NULL │ RETRACT         │ 019c7bbd-7937-718c-a1ed-e13665ed7852 │
// │ 58bcf9df-00a2-4c23-82e4-e7460234986d │ 0.9999146461486816 │ ASSERT          │ 019c7bbd-7937-719f-a68d-b1eeffa5ffa7 │
// │ 9652cb97-f0c2-490d-8aa7-966a4718a035 │               NULL │ RETRACT         │ 019c7bbd-7937-71bd-ae14-7f0a0644fe3f │
// ├──────────────────────────────────────┴────────────────────┴─────────────────┴──────────────────────────────────────┤
// │ 100 rows (40 shown)                                                                                      4 columns │
// └────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
// Run Time (s): real 0.345 user 3.456738 sys 0.536556
// ```

// Note, arg_max will ignore NULL values, so the retraction is suppressed
const scooter_pitch_rollup_with_retract_suppressed_view = `
SELECT
    id,

    -- 1. Filter down to only ASSERT and RETRACT events
    -- 2. Find the row with the highest 't' among those
    -- 3. If that row is an ASSERT, return pval. If it's a RETRACT, return NULL.
    arg_max(
        CASE WHEN pop = 'ASSERT' THEN pval ELSE NULL END,
        t
    ) FILTER (WHERE pop IN ('ASSERT', 'RETRACT')) AS current_pitch,

	-- The actual operation that resulted in the current_pitch state
    arg_max(pop, t) FILTER (WHERE pop IN ('ASSERT', 'RETRACT')) AS last_applied_op,

    -- Optional: The timestamp when this specific state was asserted/retracted
    max(t) FILTER (WHERE pop IN ('ASSERT', 'RETRACT')) AS state_updated_at,

    max(t) AS last_event_at

FROM scooter_pitch
GROUP BY id;
`

const scooter_pitch_lastvalue_view = `
SELECT
    id,
    t,
    pop,
    pval AS event_pval,

    -- 1. Map 'IGNORE' to NULL
    -- 2. Use IGNORE NULLS to carry forward the last seen non-null value
    last_value(
		CASE
			WHEN pop = 'ASSERT'
			THEN pval
			ELSE NULL
		END IGNORE NULLS
	) OVER (
		PARTITION BY id
		ORDER BY t
		ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
	) AS effective_pitch

FROM scooter_pitch
ORDER BY t;
`

const scooter_ptich_lastvalue_with_retract_view = `
SELECT
    id,
    t,
    pop,
    pval AS event_pval,

    -- Extract the 'v' (value) from the struct returned by the window function
    (
        last_value(
            CASE
                -- Pack the asserted value into a struct
                WHEN pop = 'ASSERT' THEN {'v': pval}

                -- Pack a typed NULL into a struct
                WHEN pop = 'RETRACT' THEN {'v': NULL::DOUBLE}

                -- Standard NULL. This triggers the 'IGNORE NULLS' behavior
                ELSE NULL
            END IGNORE NULLS
        ) OVER (
            PARTITION BY id
            ORDER BY t
        )
    ).v AS effective_pitch

FROM scooter_pitch
ORDER BY id, t;
`

// TestNewScooterUpdate creates a number of scooter updates and writes them to an OCF file.
//
// Create a parquet file from the avro file:
// ```
// time python3 ../avro_delta.py --avro-deltaflow-path /tmp/scooter_struct_updates.avro --parquet-deltaflow-path /tmp/scooter_struct_updates.convert.parquet --mode=convert-to-parquet
// ```
func TestNewScooterUpdate(t *testing.T) {
	defer ConfigTeardown()

	// bootstrap Avro
	_, _, schema_scooter, _ := schema_fixtures(t, false)
	register_types(t)

	// Create a new OCF file
	tmpDir := t.TempDir()
	delta_avro_path := filepath.Join(tmpDir, "delta.avro")
	f, err := os.Create(delta_avro_path)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(delta_avro_path)

	// Create a new encoder (using the string schema, rather than a pre-parsed schema)
	enc, err := ocf.NewEncoderWithSchema(schema_scooter, f)
	if err != nil {
		t.Fatal(err)
	}
	// defer enc.Close()

	// create a number of scooters
	const num_scooters = 100
	t.Logf("Creating %d scooters", num_scooters)
	scooters := make([]uuid.UUID, num_scooters)
	for i := range scooters {
		scooters[i] = uuid.New()
	}

	// create a number of scooter updates
	const scooter_updates = 9_000_000
	t.Logf("Creating %d scooter updates", scooter_updates)
	for i := range scooter_updates {

		s := NewScooterUpdate()
		if s == nil {
			t.Errorf("NewScooterUpdate() = nil, want non-nil")
		}
		// set the scooter id
		n := i % len(scooters)
		s.ID = scooters[n].String()

		// set the event id
		uuid, err := uuid.NewV7()
		if err != nil {
			t.Fatal(err)
		}
		s.TransactionID = uuid.String()

		// pick a random number between 1 and 4
		branch := rand.Intn(5) + 1

		// helper to generate a random float32 in the range [start, end)
		range_float32 := func(i int, start, end float32) float32 {
			return start + (float32(i)/float32(scooter_updates))*(end-start)
		}

		// set the delta
		// (Based on the branch, the reset of the entries are left as "ignore")
		switch branch {
		case 1:
			s.ActivityState.Op = string(deltaflow.DeltaOpAssert)
			s.ActivityState.Value = "active"
			s.Pitch.Op = string(deltaflow.DeltaOpAssert)
			s.Pitch.Value = range_float32(i, -1.0, 1.0)
			s.Roll.Op = string(deltaflow.DeltaOpAssert)
			s.Roll.Value = range_float32(i, -1.0, 1.0)
		case 2:
			s.SpeedometerSpeed.Op = string(deltaflow.DeltaOpAssert)
			s.SpeedometerSpeed.Value = range_float32(i, 3.0, 13.0)
			s.MotorSpeed.Op = string(deltaflow.DeltaOpAssert)
			s.MotorSpeed.Value = range_float32(i, 4.0, 14.0)
			s.MotorTemperature.Op = string(deltaflow.DeltaOpAssert)
			s.MotorTemperature.Value = range_float32(i, 5.0, 15.0)
		case 3:
			s.SerialNumber.Op = string(deltaflow.DeltaOpAssert)
			s.SerialNumber.Value = "1234567890"
			s.AssignedRider.Op = string(deltaflow.DeltaOpAssert)
			s.AssignedRider.Value = "1234567891"
			s.ConnectedBatterySerial.Op = string(deltaflow.DeltaOpAssert)
			s.ConnectedBatterySerial.Value = "1234567892"
		case 4:
			s.Location.Op = string(deltaflow.DeltaOpAssert)
			s.Location.Value = GeoPoint{Lat: 1.0, Lon: 2.0}
		case 5:
			// retract pitch updates... so far
			s.Pitch.Op = string(deltaflow.DeltaOpRetract)
		}

		// write the scooter update to the OCF update file
		if err := enc.Encode(s); err != nil {
			t.Fatal(err)
		}
	}

	// close and flush
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	t.Logf("Created %d scooter updates", scooter_updates)

	// check that a non-empty file was created
	info, err := os.Stat(delta_avro_path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Errorf("delta_avro_path is empty")
	}

	// create copies for review
	if true {
		t.Logf("Copying Avro and Parquet files to /tmp for off-line review")
		// Copy the parquet and avro OCF files to a new location for off-line review
		if err := common.CopyFile(delta_avro_path, "/tmp/scooter_struct_updates.avro"); err != nil {
			t.Fatal(err)
		}
	}
}
