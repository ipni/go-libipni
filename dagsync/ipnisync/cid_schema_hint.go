package ipnisync

const (
	// CidSchemaHeader is the HTTP header used as an optional hint about the
	// type of data requested by a CID.
	CidSchemaHeader = "Ipni-Cid-Schema-Type"
	// CidSchemaAd is a value for the CidSchemaHeader specifying advertiesement
	// data is being requested.
	CidSchemaAd = "advertisement"
	// CidSchemaEntries is a value for the CidSchemaHeader specifying
	// advertisement entries (multihash chunks) data is being requested.
	CidSchemaEntries = "entries"
)

// cidSchemaTypeKey is the type used for the key of CidSchemaHeader when set as
// a context value.
type cidSchemaTypeCtxKey string

// CidSchemaCtxKey is used as the key when creating a context with a value or extracting the cid schema from a context. Examples:
//
//	ctx := context.WithValue(ctx, CidSchemaCtxKey, CidSchemaAd)
//
//	cidSchemaType, ok := ctx.Value(CidSchemaCtxKey).(string)
const CidSchemaCtxKey cidSchemaTypeCtxKey = CidSchemaHeader
