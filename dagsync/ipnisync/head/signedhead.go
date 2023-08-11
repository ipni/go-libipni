package head

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

type SignedHead struct {
	Head   ipld.Link
	Topic  *string
	Pubkey []byte
	Sig    []byte
}

var (
	ErrBadSignature = errors.New("invalid signature")
	ErrNoSignature  = errors.New("missing signature")
	ErrNoPubkey     = errors.New("missing public key")
)

func NewSignedHead(headCid cid.Cid, topic string, privKey ic.PrivKey) (*SignedHead, error) {
	sh := &SignedHead{
		Head: ipld.Link(cidlink.Link{Cid: headCid}),
	}
	if topic != "" {
		sh.Topic = &topic
	}
	err := sh.Sign(privKey)
	if err != nil {
		return nil, err
	}
	return sh, nil
}

func Decode(SignedHeadReader io.Reader) (*SignedHead, error) {
	builder := SignedHeadPrototype.NewBuilder()
	err := dagjson.Decode(builder, SignedHeadReader)
	if err != nil {
		return nil, err
	}
	return UnwrapSignedHead(builder.Build())
}

func (s SignedHead) Encode() ([]byte, error) {
	node, err := s.ToNode()
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	err = dagjson.Encode(node, &buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ToNode converts this SignedHead to its representation as an IPLD typed node.
//
// See: bindnode.Wrap.
func (s SignedHead) ToNode() (n ipld.Node, err error) {
	// TODO: remove the panic recovery once IPLD bindnode is stabilized.
	defer func() {
		if r := recover(); r != nil {
			err = toError(r)
		}
	}()
	n = bindnode.Wrap(&s, SignedHeadPrototype.Type()).Representation()
	return
}

// UnwrapSignedHead unwraps the given node as a SignedHead.
//
// Note that the node is reassigned to SignedHeadPrototype if its prototype is
// different. Therefore, it is recommended to load the node using the correct
// prototype function initially to avoid unnecessary node assignment.
func UnwrapSignedHead(node ipld.Node) (*SignedHead, error) {
	// When an IPLD node is loaded using `Prototype.Any` unwrap with bindnode
	// will not work. Here we defensively check the prototype and wrap if
	// needed.
	//
	// The code in this repo, however should load nodes with appropriate
	// prototype and never trigger this if statement.
	if node.Prototype() != SignedHeadPrototype {
		headBuilder := SignedHeadPrototype.NewBuilder()
		err := headBuilder.AssignNode(node)
		if err != nil {
			return nil, fmt.Errorf("faild to convert node prototype: %w", err)
		}
		node = headBuilder.Build()
	}

	head, ok := bindnode.Unwrap(node).(*SignedHead)
	if !ok || head == nil {
		return nil, fmt.Errorf("unwrapped node does not match schema.SignedHead")
	}
	return head, nil
}

// Validate checks the signature on the SignedHead and returns the peer ID of
// the signer.
//
// Even though the signature is valid with the included public key, the caller
// will need to check if the signer's ID is allowed to sign this message.
func (s SignedHead) Validate() (peer.ID, error) {
	if len(s.Sig) == 0 {
		return "", ErrNoSignature
	}
	if len(s.Pubkey) == 0 {
		return "", ErrNoPubkey
	}

	pubKey, err := ic.UnmarshalPublicKey(s.Pubkey)
	if err != nil {
		return "", err
	}

	var sigBuf bytes.Buffer
	cidBytes := s.Head.(cidlink.Link).Cid.Bytes()
	var topicLen int
	if s.Topic != nil {
		topicLen = len(*s.Topic)
	}
	sigBuf.Grow(len(cidBytes) + topicLen)
	sigBuf.Write(cidBytes)
	if topicLen != 0 {
		sigBuf.WriteString(*s.Topic)
	}
	ok, err := pubKey.Verify(sigBuf.Bytes(), s.Sig)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", ErrBadSignature
	}

	// Get peer ID of signer so that caller can check that it is allowed.
	signerID, err := peer.IDFromPublicKey(pubKey)
	if err != nil {
		return "", fmt.Errorf("cannot convert public key to peer ID: %w", err)
	}

	return signerID, nil
}

func (s *SignedHead) Sign(privKey ic.PrivKey) error {
	var sigBuf bytes.Buffer
	cidBytes := s.Head.(cidlink.Link).Cid.Bytes()
	var topicLen int
	if s.Topic != nil {
		topicLen = len(*s.Topic)
	}
	sigBuf.Grow(len(cidBytes) + topicLen)
	sigBuf.Write(cidBytes)
	if topicLen != 0 {
		sigBuf.WriteString(*s.Topic)
	}
	var err error
	s.Pubkey, err = ic.MarshalPublicKey(privKey.GetPublic())
	if err != nil {
		return err
	}
	s.Sig, err = privKey.Sign(sigBuf.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func toError(r any) error {
	switch x := r.(type) {
	case string:
		return errors.New(x)
	case error:
		return x
	default:
		return fmt.Errorf("unknown panic: %v", r)
	}
}
