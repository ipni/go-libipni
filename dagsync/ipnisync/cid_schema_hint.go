package ipnisync

import (
	"context"
	"errors"
)

const (
	// CidSchemaHeader is the HTTP header used as an optional hint about the
	// type of data requested by a CID.
	CidSchemaHeader = "Ipni-Cid-Schema-Type"
	// CidSchemaAdvertisement is a value for the CidSchemaHeader specifying
	// advertiesement data is being requested. Referrs to Advertisement in
	// https://github.com/ipni/go-libipni/blob/main/ingest/schema/schema.ipldsch
	CidSchemaAdvertisement = "Advertisement"
	// CidSchemaEntries is a value for the CidSchemaHeader specifying
	// advertisement entries (multihash chunks) data is being requested.
	// Referrs to Entry chunk in
	// https://github.com/ipni/go-libipni/blob/main/ingest/schema/schema.ipldsch
	CidSchemaEntryChunk = "EntryChunk"
)

var ErrUnknownCidSchema = errors.New("unknown cid schema type value")

// cidSchemaTypeKey is the type used for the key of CidSchemaHeader when set as
// a context value.
type cidSchemaTypeCtxKey string

// cidSchemaCtxKey is used to get the key used to store or extract the cid
// schema value in a context.
const cidSchemaCtxKey cidSchemaTypeCtxKey = CidSchemaHeader

// CidSchemaFromCtx extracts the CID schema name from the context. If the
// scheam value is not set, then returns "". If the schema value is set, but is
// not recognized, then ErrUnknownCidSchema is returned along with the value.
//
// Returning unrecognized values with an  error allows consumers to retrieved
// newer values that are not recognized by an older version of this library.
func CidSchemaFromCtx(ctx context.Context) (string, error) {
	if ctx == nil {
		return "", nil
	}
	cidSchemaType, ok := ctx.Value(cidSchemaCtxKey).(string)
	if !ok {
		return "", nil
	}

	var err error
	switch cidSchemaType {
	case CidSchemaAdvertisement, CidSchemaEntryChunk:
	default:
		err = ErrUnknownCidSchema
	}
	return cidSchemaType, err
}

// CtxWithCidSchema creates a derived context that has the specified value for
// the CID schema type.
//
// Setting an unrecognized value, even when an error is retruned, allows
// producers to set context values that are not recognized by an older version
// of this library.
func CtxWithCidSchema(ctx context.Context, cidSchemaType string) (context.Context, error) {
	if cidSchemaType == "" {
		return ctx, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var err error
	switch cidSchemaType {
	case CidSchemaAdvertisement, CidSchemaEntryChunk:
	default:
		err = ErrUnknownCidSchema
	}
	return context.WithValue(ctx, cidSchemaCtxKey, cidSchemaType), err
}
