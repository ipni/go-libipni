// Package schema defines the makeup of an advertisement. An advertisement is
// used to inform an indexer that there are updates to a provider's content.
// The IsRm and Entries fields determine how the indexer handles the
// advertisement, as described by the following table:
//
//	IsRm    Entries    Action
//
// ------+-----------+------------------------------------
// false | NoEntries | Update metadata
// false | data      | Update metadata and index entries
// true  | NoEntries | Delete content with context ID
// true  | data      | Delete content with context ID (ignore entries)
//
// When removing content (IsRm true) the metadata and entries are ignored. An
// advertisement that removes content cannot have extended provider
// information.
//
// All advertisements update the provider's addresses. To create an
// advertisement that only updates a provider's address, create an
// advertisement that contains no metadata and no entries.
package schema
