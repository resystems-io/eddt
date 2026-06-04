package basic_sha256

// SHA256Snapshot is used to verify --standalone-hash sha256 generation. The
// resulting companion file must include the SHA-256 warning comment.
//
//go:generate delta-gen --standalone --standalone-hash sha256 SHA256Snapshot
type SHA256Snapshot struct {
	ID    string `eddt:"entity.key"`
	Score int32
}
