package metadata

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-varint"
)

const MaxMetadataSize = 1024

var (
	ErrTooLong = errors.New("too long")
)

// Unknown represents an unparsed metadata payload
type Unknown struct {
	Code    multicodec.Code
	Payload []byte
}

func (u *Unknown) ID() multicodec.Code {
	return u.Code
}

func (u *Unknown) MarshalBinary() ([]byte, error) {
	return u.Payload, nil
}

func (u *Unknown) UnmarshalBinary(data []byte) error {
	r := bytes.NewReader(data)
	_, err := u.ReadFrom(r)
	return err
}

func (u *Unknown) ReadFrom(r io.Reader) (int64, error) {
	cr := &countingReader{r: r}
	v, err := varint.ReadUvarint(cr)
	if err != nil {
		return cr.readCount, err
	}
	u.Code = multicodec.Code(v)

	size, err := varint.ReadUvarint(cr)
	if err != nil {
		return cr.readCount, err
	}

	codeSize := varint.UvarintSize(v)
	sizeSize := varint.UvarintSize(size)
	buf := make([]byte, codeSize+sizeSize+int(size))
	varint.PutUvarint(buf, v)
	varint.PutUvarint(buf[codeSize:], size)
	n, err := r.Read(buf[codeSize+sizeSize:])
	readLen := codeSize + sizeSize + n
	if err != nil {
		u.Payload = buf[:readLen]
		return int64(readLen), err
	}
	if size != uint64(n) {
		u.Payload = buf[:readLen]
		return int64(readLen), fmt.Errorf("expected %d readable bytes but read %d", size, n)
	}
	u.Payload = buf

	return int64(readLen), nil
}
