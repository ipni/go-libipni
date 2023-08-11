package head

import (
	_ "embed"
	"fmt"

	"github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	_ "github.com/ipld/go-ipld-prime/codec/raw"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
)

// SignedHeadPrototype represents the IPLD node prototype of SignedHead.
//
// See: bindnode.Prototype.
var SignedHeadPrototype schema.TypedPrototype

//go:embed schema.ipldsch
var schemaBytes []byte

func init() {
	typeSystem, err := ipld.LoadSchemaBytes(schemaBytes)
	if err != nil {
		panic(fmt.Errorf("failed to load schema: %w", err))
	}
	SignedHeadPrototype = bindnode.Prototype((*SignedHead)(nil), typeSystem.TypeByName("SignedHead"))
}
