package model

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

// ProviderResult is a one of possibly multiple results when looking up a
// provider of indexed context.
type ProviderResult struct {
	// ContextID identifies the metadata that is part of this value.
	ContextID []byte `json:"ContextID,omitempty"`
	// Metadata contains information for the provider to use to retrieve data.
	Metadata []byte `json:"Metadata,omitempty"`
	// Provider is the peer ID and addresses of the provider.
	Provider *peer.AddrInfo `json:"Provider,omitempty"`
}

// MultihashResult aggregates all values for a single multihash.
type MultihashResult struct {
	Multihash       multihash.Multihash
	ProviderResults []ProviderResult
}

// FindResponse used to answer client queries/requests
type FindResponse struct {
	MultihashResults          []MultihashResult          `json:"MultihashResults,omitempty"`
	EncryptedMultihashResults []EncryptedMultihashResult `json:"EncryptedMultihashResults,omitempty"`
	// NOTE: This feature is not enabled yet.
	// Signature []byte	// Providers signature.
}

// EncryptedMultihashResult aggregates all encrypted value keys for a single multihash
type EncryptedMultihashResult struct {
	Multihash          multihash.Multihash `json:"Multihash,omitempty"`
	EncryptedValueKeys [][]byte            `json:"EncryptedValueKeys,omitempty"`
}

// Equal compares ProviderResult values to determine if they are equal. The
// provider addresses are omitted from the comparison.
func (pr ProviderResult) Equal(other ProviderResult) bool {
	if !bytes.Equal(pr.ContextID, other.ContextID) {
		return false
	}
	if !bytes.Equal(pr.Metadata, other.Metadata) {
		return false
	}
	if pr.Provider.ID != other.Provider.ID {
		return false
	}
	return true
}

// MarshalFindResponse serializes a find response.
func MarshalFindResponse(r *FindResponse) ([]byte, error) {
	return json.Marshal(r)
}

// UnmarshalFindResponse de-serializes a find response.
func UnmarshalFindResponse(b []byte) (*FindResponse, error) {
	r := &FindResponse{}
	err := json.Unmarshal(b, r)
	return r, err
}

func (r *FindResponse) String() string {
	var b strings.Builder
	for i := range r.MultihashResults {
		data, err := json.MarshalIndent(&r.MultihashResults[i], "", "  ")
		if err != nil {
			return err.Error()
		}
		b.Write(data)
		b.WriteByte(0x0a)
	}
	return b.String()
}

// PrettyPrint a response for CLI output
func (r *FindResponse) PrettyPrint() {
	fmt.Println(r.String())
}
