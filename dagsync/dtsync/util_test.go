package dtsync

import (
	"testing"

	"github.com/ipfs/go-datastore"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/dagsync/test"
	"github.com/stretchr/testify/require"
)

func Test_registerVoucherHandlesAlreadyRegisteredGracefully(t *testing.T) {
	h := test.MkTestHost(t)
	dt, _, close, err := makeDataTransfer(h, datastore.NewMapDatastore(), cidlink.DefaultLinkSystem(), nil, 0, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, close()) })

	require.NoError(t, registerVoucher(dt, nil))
	require.NoError(t, registerVoucher(dt, nil))
}
