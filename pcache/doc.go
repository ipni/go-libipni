// Package pcache provides a lock-free provider information cache for high
// performance concurrent reads.
//
// ProviderCache caches provider information from one or more sources of
// information. Cached data is looked up and returned concurrently without lock
// contention. This enables services that require high-frequency concurrent
// cache reads, such as those that must quickly deliver IPNI find results, or
// that handle a high volume of provider information requests.
//
// # Multiple Overlapping Data Sources
//
// The cache can be configured with multiple data sources from which provider
// information is fetched. If the same provider information is returned from
// multiple data sources, then the information with the most recent timestamp
// is used.
//
// # All Provider Information Cached
//
// The provider cache maintains a unified view of all provider information
// across all data sources. Caching provider information in builk allows the
// cached data to be refreshed with fewer fetches over the network, then if
// information was fetched for individual providers. This is particularly
// important when there are multiple sources to fetch provider information
// from, and when responses include infomration about multiple providers.
//
// # Cache Refresh
//
// If the cache refresh interval is non-zero (default is 2 minutes), then after
// that time is elapsed, a timer sets a flag to indicate that refresh is
// required. The next cache lookup checks the flag and if set, begins a refresh
// asynchronously (in the background). When the refresh is done, the cached
// read-only data is updates and the refresh timer is reset. This way there is
// no need to have a separate goroutine to manage, and no unnecessary refresh
// work is done if the cache is not actively used. A refresh can also be
// explicitly requested.
//
// Each refresh updates provider information from all sources. An updated view
// of the cached data is built, and is then atomically set as the cache's
// read-only lock-free data.
//
// # Negative Cache
//
// Lookups for provider information that are not currently cached will generate
// an initial query to the cache's data sources. If the information is found
// then it is cached. If not, a negative cache entry is cached. The negative
// cache entry prevents subsequent queries for the same provider from querying
// data sources. If the information becomes available the negative cache entry
// is replaced at the next refresh.
//
// # Cache Eviction
//
// Cached provider information remains in the cache until the information is no
// longer available from any of the sources for longer than the configured
// time-to-live. The time-to-live countdown begins when the information is no
// longer seen from any source. Negative cache entries are also evicted after
// having been in the cache for the time-to-live.
package pcache
