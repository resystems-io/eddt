package with_header_rejected

import eddt "go.resystems.io/eddt/runtime"

// HeaderSnapshot embeds runtime.Header. The parser must reject this struct
// when --standalone is set, since standalone mode requires plain structs
// without any chain-lifecycle envelope.
type HeaderSnapshot struct {
	eddt.Header
	ID string `eddt:"entity.key"`
}
