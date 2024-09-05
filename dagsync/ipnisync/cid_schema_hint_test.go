package ipnisync_test

import (
	"context"
	"testing"

	"github.com/ipni/go-libipni/dagsync/ipnisync"
	"github.com/stretchr/testify/require"
)

func TestCtxWithCidSchema(t *testing.T) {
	ctxOrig := context.Background()
	ctx, err := ipnisync.CtxWithCidSchema(ctxOrig, "")
	require.NoError(t, err)
	require.Equal(t, ctxOrig, ctx)

	ctx, err = ipnisync.CtxWithCidSchema(ctxOrig, ipnisync.CidSchemaAdvertisement)
	require.NoError(t, err)
	require.NotEqual(t, ctxOrig, ctx)

	value, err := ipnisync.CidSchemaFromCtx(ctx)
	require.NoError(t, err)
	require.Equal(t, ipnisync.CidSchemaAdvertisement, value)

	ctx, err = ipnisync.CtxWithCidSchema(ctx, ipnisync.CidSchemaEntryChunk)
	require.NoError(t, err)
	value, err = ipnisync.CidSchemaFromCtx(ctx)
	require.NoError(t, err)
	require.Equal(t, ipnisync.CidSchemaEntryChunk, value)

	value, err = ipnisync.CidSchemaFromCtx(ctxOrig)
	require.NoError(t, err)
	require.Empty(t, value)

	const unknownVal = "unknown"

	// Setting unknown value returns error as well as context with value.
	ctx, err = ipnisync.CtxWithCidSchema(ctxOrig, unknownVal)
	require.ErrorIs(t, err, ipnisync.ErrUnknownCidSchema)
	require.NotNil(t, ctxOrig, ctx)

	// Getting unknown value returns error as well as value.
	value, err = ipnisync.CidSchemaFromCtx(ctx)
	require.ErrorIs(t, err, ipnisync.ErrUnknownCidSchema)
	require.Equal(t, unknownVal, value)
}
