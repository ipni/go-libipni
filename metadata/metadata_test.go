package metadata_test

import (
	"math/rand"
	"testing"

	"github.com/ipfs/go-test/random"
	"github.com/ipni/go-libipni/metadata"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-varint"
	"github.com/stretchr/testify/require"
)

func TestMetadata(t *testing.T) {
	cids := random.Cids(4)
	tests := []struct {
		name            string
		givenTransports []metadata.Protocol
		wantValidateErr string
	}{
		{
			name: "Out of order transports",
			givenTransports: []metadata.Protocol{
				&metadata.GraphsyncFilecoinV1{
					PieceCID:      cids[0],
					VerifiedDeal:  false,
					FastRetrieval: false,
				},
				&metadata.Bitswap{},
			},
		},
		{
			name:            "No transports is invalid",
			wantValidateErr: "at least one transport must be specified",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			subject := metadata.Default.New(test.givenTransports...)
			require.Equal(t, len(test.givenTransports), subject.Len())

			err := subject.Validate()
			if test.wantValidateErr != "" {
				require.EqualError(t, err, test.wantValidateErr)
				return
			}
			require.NoError(t, err)

			tps := test.givenTransports
			rand.Shuffle(len(tps), func(i, j int) { tps[i], tps[j] = tps[j], tps[i] })

			// Assert transports are sorted
			anotherSubject := metadata.Default.New(tps...)
			require.Equal(t, subject, anotherSubject)

			gotBytes, err := subject.MarshalBinary()
			require.NoError(t, err)

			subjectFromBytes := metadata.Default.New()
			err = subjectFromBytes.UnmarshalBinary(gotBytes)
			require.NoError(t, err)
			require.Equal(t, subject, subjectFromBytes)
			require.True(t, subject.Equal(subjectFromBytes))

			err = subject.Validate()
			if test.wantValidateErr != "" {
				require.EqualError(t, err, test.wantValidateErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestMetadata_UnmarshalBinary(t *testing.T) {
	unknownID := multicodec.Libp2pRelayRsvp
	unknownData := makeUnknownData(unknownID)
	unknownMetadata := &metadata.Unknown{
		Code:    unknownID,
		Payload: unknownData,
	}

	tests := []struct {
		name         string
		givenBytes   []byte
		wantMetadata metadata.Metadata
		wantErr      string
	}{
		{
			name:    "Empty bytes is error",
			wantErr: "at least one transport must be specified",
		},
		{
			name:         "Unknown transport ID is not error",
			givenBytes:   unknownData,
			wantMetadata: metadata.Default.New(unknownMetadata),
		},
		{
			name:         "Known transport ID is not error",
			givenBytes:   varint.ToUvarint(uint64(multicodec.TransportBitswap)),
			wantMetadata: metadata.Default.New(&metadata.Bitswap{}),
		},

		{
			name:         "Known transport ID mixed with unknown ID is not error",
			givenBytes:   append(unknownData, varint.ToUvarint(uint64(multicodec.TransportBitswap))...),
			wantMetadata: metadata.Default.New(unknownMetadata, &metadata.Bitswap{}),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			subject := metadata.Default.New()
			err := subject.UnmarshalBinary(test.givenBytes)
			if test.wantErr == "" {
				require.NoError(t, err)
				require.Equal(t, subject, test.wantMetadata)
				require.NoError(t, subject.Validate())
			} else {
				require.EqualError(t, err, test.wantErr)
			}
		})
	}
}

func makeUnknownData(code multicodec.Code) []byte {
	id := uint64(code)
	contents := []byte("hello")
	codeSize := varint.UvarintSize(id)
	sizeSize := varint.UvarintSize(uint64(len(contents)))
	data := make([]byte, codeSize+sizeSize+len(contents))
	varint.PutUvarint(data, id)
	varint.PutUvarint(data[codeSize:], uint64(len(contents)))
	copy(data[codeSize+sizeSize:], contents)
	return data
}
