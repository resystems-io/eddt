package deltaflow

import (
	_ "embed"
)

//go:generate avrogen -pkg deltaflow -enums -o avro_delta_type.go -tags json:snake,yaml:upper-camel avro_delta.avsc

//go:embed avro_delta.avsc
var avro_delta_schema []byte

//go:embed avro_geo.avsc
var avro_geo_schema []byte
