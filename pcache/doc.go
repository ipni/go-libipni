// Package pcache provides a lock-free provider information bulk cache for high
// performance concurrent reads.
//
// The provider cache maintains a unifiied view of all provider information
// across all data sources. Caching provider information in builk allows the
// cached data to be refreshed with fewer fetches over the network.
//
// ProviderCache caches provider addresses and extended provider information.
// This is the provider information that is needed for IPNI query results. This
// cached data can be concurrently fetched without lock contention. This allows
// for high-frequency concurrent cache reads, as is needed for services
// delivering IPNI query results.
//
// ## Cache Refresh
//
// A goroutine performs periodic and on-demand cache refreshes to keep the cache
// up-to-date with provider information from all sources. This goroutine builds
// an updated view of the cached data, and then atomically sets this view as
// the cache's read-only lock-free data.
//
// ## Negative Cache
//
// Lookups for provider information that are not currently cached will generate
// an initial query to the cache's data sources. If the information is found
// then it is cached. If not, a negative cache entry is cached. The negative
// cache entry prevents subsequent queries for the same provider from querying
// data sources. If the information becomes available the negative cache entry
// is replaced at the next refresh.
//
// ## Cache Eviction
//
// Cached provider information remains in the cache until the information is no
// longer available from any of the sources, for longer then the configured
// time-to-live. The time-to-live count-down begins when the information is seen
// to be no longer available. Negative cache entries are also evicted after
// having been in the cache for the time-to-live.
//
// ## Multiple Overlapping Data Sources
//
// The cache can be configured with multiple data sources from which provider
// information is fetched. If the same provider information is returned from
// multiple data sources, then the information with the most recent timestamp is
// used.
package pcache
