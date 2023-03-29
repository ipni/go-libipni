package main

import (
	"github.com/ipni/go-libipni/announce/message"
	cbg "github.com/whyrusleeping/cbor-gen"
)

func main() {
	err := cbg.WriteTupleEncodersToFile("cbor_gen.go", "message", message.Message{})
	if err != nil {
		panic(err)
	}
}
