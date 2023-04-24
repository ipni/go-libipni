package metadata

import (
	"bytes"
	"fmt"
	"io"

	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-varint"
)

var (
	ipfsGatewayHttpBytes           = append(varint.ToUvarint(uint64(multicodec.TransportIpfsGatewayHttp)), ipfsGatewayHttpPayloadLenBytes...)
	ipfsGatewayHttpPayloadLenBytes = varint.ToUvarint(0)

	_ Protocol = (*IpfsGatewayHttp)(nil)
)

// IpfsGatewayHttp represents the indexing metadata that uses multicodec.TransportIpfsGatewayHttp.
type IpfsGatewayHttp struct {
}

func (b IpfsGatewayHttp) ID() multicodec.Code {
	return multicodec.TransportIpfsGatewayHttp
}

func (b IpfsGatewayHttp) MarshalBinary() ([]byte, error) {
	return ipfsGatewayHttpBytes, nil
}

func (b IpfsGatewayHttp) UnmarshalBinary(data []byte) error {
	if !bytes.Equal(data, ipfsGatewayHttpBytes) {
		return fmt.Errorf("transport ID does not match %s", multicodec.TransportIpfsGatewayHttp)
	}
	return nil
}

func (b IpfsGatewayHttp) ReadFrom(r io.Reader) (n int64, err error) {
	wantLen := len(ipfsGatewayHttpBytes)
	buf := make([]byte, wantLen)
	read, err := r.Read(buf)
	bRead := int64(read)
	if err != nil {
		return bRead, err
	}
	if wantLen != read {
		return bRead, fmt.Errorf("expected %d readable bytes but read %d", wantLen, read)
	}

	if !bytes.Equal(ipfsGatewayHttpBytes, buf) {
		return bRead, fmt.Errorf("transport ID does not match %s", multicodec.TransportIpfsGatewayHttp)
	}
	return bRead, nil
}
