package metadata_test

import (
	"testing"

	"github.com/ipfs/go-test/random"
	"github.com/ipni/go-libipni/metadata"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-varint"
	"github.com/stretchr/testify/require"
)

func TestRoundTripDataTransferFilecoin(t *testing.T) {
	cids := random.Cids(4)
	filecoinV1Datas := []*metadata.GraphsyncFilecoinV1{
		{
			PieceCID:      cids[0],
			VerifiedDeal:  false,
			FastRetrieval: false,
		},
		{
			PieceCID:      cids[1],
			VerifiedDeal:  false,
			FastRetrieval: true,
		},
		{
			PieceCID:      cids[2],
			VerifiedDeal:  true,
			FastRetrieval: true,
		},
		{
			PieceCID:      cids[3],
			VerifiedDeal:  true,
			FastRetrieval: true,
		},
	}
	for _, src := range filecoinV1Datas {
		require.Equal(t, multicodec.TransportGraphsyncFilecoinv1, src.ID())

		asBytes, err := src.MarshalBinary()
		require.NoError(t, err)

		dst := &metadata.GraphsyncFilecoinV1{}
		err = dst.UnmarshalBinary(asBytes)
		require.NoError(t, err)
		require.Equal(t, src, dst)
	}
}

func TestGraphsyncFilecoinV1Metadata_FromIndexerMetadataErr(t *testing.T) {
	dst := &metadata.GraphsyncFilecoinV1{}
	err := dst.UnmarshalBinary(varint.ToUvarint(uint64(multicodec.TransportBitswap)))
	require.Errorf(t, err, "invalid transport ID: transport-bitswap")
}
