package conformance

//go:generate go run go.resystems.io/eddt/cmd/arrow-writer-gen -p . -s FallbackRecord,StrictRecord -o fallback-arrow-writer.go -n conformance -v
//go:generate go run go.resystems.io/eddt/cmd/arrow-reader-gen -p . -s FallbackRecord,StrictRecord -o fallback-arrow-reader.go -n conformance -v

import (
	"net"
	"net/netip"

	"google.golang.org/protobuf/types/descriptorpb"

	"go.resystems.io/eddt/internal/arrow/conformance/external"
)

// StrictHolder nests a Strict field one struct level down, so a malformed
// payload inside it shows how the reader reports the field path.
type StrictHolder struct {
	S external.Strict
}

// FallbackRecord references only types the generator cannot resolve
// natively, so every field goes through the external-type fallback chain.
// The external package documents each type. The standard library and
// protobuf fields are there because they are the real-world versions of the
// same shapes: netip.Addr and net.IP marshal to text, and
// descriptorpb.FieldDescriptorProto_Type is a protoc-generated enum, a named
// int32 whose only fallback method is String. External types as generic type arguments, and named slices of
// external types, are absent on purpose: the generator emits code for them
// that does not compile, which generator_test.go shows.
type FallbackRecord struct {
	Text    external.TextOnly
	PText   *external.TextOnly
	Texts   []external.TextOnly
	TextMap map[string]external.TextOnly
	Str     external.StringerOnly
	PStr    *external.StringerOnly
	Bin     external.BinaryOnly
	PBin    *external.BinaryOnly
	Both    external.TextAndBinary
	PtrRecv external.PointerText
	Addr    netip.Addr
	PAddr   *netip.Addr
	Prefix  netip.Prefix
	IP      net.IP
	Enum    descriptorpb.FieldDescriptorProto_Type
	Level   external.Level
	Levels  external.Levels
	Failing external.FailingMarshal
}

// StrictRecord places the Strict type in every position the reader handles
// differently, so the malformed-payload tests can compare the error path of
// each: a value field, a pointer field, a list element, a map value and a
// field of a nested struct.
type StrictRecord struct {
	ID      string
	S       external.Strict
	PS      *external.Strict
	Ss      []external.Strict
	SMap    map[string]external.Strict
	Holder  StrictHolder
	PHolder *StrictHolder
}
