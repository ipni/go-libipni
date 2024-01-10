package schema

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/multicodec"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/multiformats/go-multihash"

	// Import so these codecs get registered.
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
)

type (
	ExtendedProvider struct {
		Providers []Provider
		Override  bool
	}

	Provider struct {
		ID        string
		Addresses []string
		Metadata  []byte
		Signature []byte
	}

	Advertisement struct {
		PreviousID       ipld.Link
		Provider         string
		Addresses        []string
		Signature        []byte
		Entries          ipld.Link
		ContextID        []byte
		Metadata         []byte
		IsRm             bool
		ExtendedProvider *ExtendedProvider
	}
	EntryChunk struct {
		Entries []multihash.Multihash
		Next    ipld.Link
	}
)

// ToNode converts this advertisement to its representation as an IPLD typed node.
//
// See: bindnode.Wrap.
func (a Advertisement) ToNode() (n ipld.Node, err error) {
	// TODO: remove the panic recovery once IPLD bindnode is stabilized.
	defer func() {
		if r := recover(); r != nil {
			err = toError(r)
		}
	}()
	n = bindnode.Wrap(&a, AdvertisementPrototype.Type()).Representation()
	return
}

// UnwrapAdvertisement unwraps the given node as an advertisement.
//
// Note that the node is reassigned to AdvertisementPrototype if its prototype is different.
// Therefore, it is recommended to load the node using the correct prototype initially
// function to avoid unnecessary node assignment.
func UnwrapAdvertisement(node ipld.Node) (*Advertisement, error) {
	// When an IPLD node is loaded using `Prototype.Any` unwrap with bindnode will not work.
	// Here we defensively check the prototype and wrap if needed, since:
	//   - linksystem in sti is passed into other libraries, and
	//   - for whatever reason clients of this package may load nodes using Prototype.Any.
	//
	// The code in this repo, however should load nodes with appropriate prototype and never trigger
	// this if statement.
	if node.Prototype() != AdvertisementPrototype {
		adBuilder := AdvertisementPrototype.NewBuilder()
		err := adBuilder.AssignNode(node)
		if err != nil {
			return nil, fmt.Errorf("faild to convert node prototype: %w", err)
		}
		node = adBuilder.Build()
	}

	ad, ok := bindnode.Unwrap(node).(*Advertisement)
	if !ok || ad == nil {
		return nil, fmt.Errorf("unwrapped node does not match schema.Advertisement")
	}
	return ad, nil
}

// Return the Advertisement's previous CID, or cid.Undef if there is no
// previous CID.
func (a Advertisement) PreviousCid() cid.Cid {
	if a.PreviousID == nil {
		return cid.Undef
	}
	return a.PreviousID.(cidlink.Link).Cid
}

func (a Advertisement) Validate() error {
	if len(a.ContextID) > MaxContextIDLen {
		return errors.New("context id too long")
	}

	if len(a.Metadata) > MaxMetadataLen {
		return errors.New("metadata too long")
	}

	return nil
}

// BytesToAdvertisement deserializes an Advertisement from a buffer. It does
// not check that the given CID matches the data, as this should have been done
// when the data was acquired.
func BytesToAdvertisement(adCid cid.Cid, data []byte) (Advertisement, error) {
	adNode, err := decodeIPLDNode(adCid.Prefix().Codec, bytes.NewBuffer(data), AdvertisementPrototype)
	if err != nil {
		return Advertisement{}, err
	}
	ad, err := UnwrapAdvertisement(adNode)
	if err != nil {
		return Advertisement{}, err
	}
	return *ad, nil
}

// BytesToEntryChunk deserializes an EntryChunk from a buffer. It does not
// check that the given CID matches the data, as this should have been done
// when the data was acquired.
func BytesToEntryChunk(entCid cid.Cid, data []byte) (EntryChunk, error) {
	entNode, err := decodeIPLDNode(entCid.Prefix().Codec, bytes.NewBuffer(data), EntryChunkPrototype)
	if err != nil {
		return EntryChunk{}, err
	}
	ent, err := UnwrapEntryChunk(entNode)
	if err != nil {
		return EntryChunk{}, err
	}
	return *ent, nil
}

// decodeIPLDNode decodes an ipld.Node from bytes read from an io.Reader.
func decodeIPLDNode(codec uint64, r io.Reader, prototype ipld.NodePrototype) (ipld.Node, error) {
	nb := prototype.NewBuilder()
	decoder, err := multicodec.LookupDecoder(codec)
	if err != nil {
		return nil, err
	}
	if err = decoder(nb, r); err != nil {
		return nil, err
	}
	return nb.Build(), nil
}

// UnwrapEntryChunk unwraps the given node as an entry chunk.
//
// Note that the node is reassigned to EntryChunkPrototype if its prototype is different.
// Therefore, it is recommended to load the node using the correct prototype initially
// function to avoid unnecessary node assignment.
func UnwrapEntryChunk(node ipld.Node) (*EntryChunk, error) {
	// When an IPLD node is loaded using `Prototype.Any` unwrap with bindnode will not work.
	// Here we defensively check the prototype and wrap if needed, since:
	//   - linksystem in sti is passed into other libraries, and
	//   - for whatever reason clients of this package may load nodes using Prototype.Any.
	//
	// The code in this repo, however should load nodes with appropriate prototype and never trigger
	// this if statement.
	if node.Prototype() != EntryChunkPrototype {
		adBuilder := EntryChunkPrototype.NewBuilder()
		err := adBuilder.AssignNode(node)
		if err != nil {
			return nil, fmt.Errorf("faild to convert node prototype: %w", err)
		}
		node = adBuilder.Build()
	}
	chunk, ok := bindnode.Unwrap(node).(*EntryChunk)
	if !ok || chunk == nil {
		return chunk, fmt.Errorf("unwrapped node does not match schema.EntryChunk")
	}
	return chunk, nil
}

// ToNode converts this entry chunk to its representation as an IPLD typed node.
//
// See: bindnode.Wrap.
func (e EntryChunk) ToNode() (n ipld.Node, err error) {
	// TODO: remove the panic recovery once IPLD bindnode is stabilized.
	defer func() {
		if r := recover(); r != nil {
			err = toError(r)
		}
	}()
	n = bindnode.Wrap(&e, EntryChunkPrototype.Type()).Representation()
	return
}

func toError(r interface{}) error {
	switch x := r.(type) {
	case string:
		return errors.New(x)
	case error:
		return x
	default:
		return fmt.Errorf("unknown panic: %v", r)
	}
}
