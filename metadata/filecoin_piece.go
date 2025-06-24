package metadata

import (
	"bytes"
	"fmt"
	"io"

	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-varint"
)

var (
	filecoinPieceHttpBytes           = append(varint.ToUvarint(uint64(multicodec.TransportFilecoinPieceHttp)), filecoinPieceHttpPayloadLenBytes...)
	filecoinPieceHttpPayloadLenBytes = varint.ToUvarint(0)

	_ Protocol = (*FilecoinPieceHttp)(nil)
)

// FilecoinPieceHttp represents the indexing metadata that uses multicodec.TransportFilecoinPieceHttp.
type FilecoinPieceHttp struct {
}

func (b FilecoinPieceHttp) ID() multicodec.Code {
	return multicodec.TransportFilecoinPieceHttp
}

func (b FilecoinPieceHttp) MarshalBinary() ([]byte, error) {
	return filecoinPieceHttpBytes, nil
}

func (b FilecoinPieceHttp) UnmarshalBinary(data []byte) error {
	if !bytes.Equal(data, filecoinPieceHttpBytes) {
		return fmt.Errorf("transport ID does not match %s", multicodec.TransportFilecoinPieceHttp)
	}
	return nil
}

func (b FilecoinPieceHttp) ReadFrom(r io.Reader) (n int64, err error) {
	wantLen := len(filecoinPieceHttpBytes)
	buf := make([]byte, wantLen)
	read, err := r.Read(buf)
	bRead := int64(read)
	if err != nil {
		return bRead, err
	}
	if wantLen != read {
		return bRead, fmt.Errorf("expected %d readable bytes but read %d", wantLen, read)
	}

	if !bytes.Equal(filecoinPieceHttpBytes, buf) {
		return bRead, fmt.Errorf("transport ID does not match %s", multicodec.TransportFilecoinPieceHttp)
	}
	return bRead, nil
}
