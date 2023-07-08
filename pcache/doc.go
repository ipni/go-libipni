// Package pcache provides a lock-free provider information cache for high
// performance concurrent reads.
//
// ProviderCache caches provider addresses and extended provider information,
// which is the information that is needed for IPNI query results. Cached data
// is retrieved concurrently without lock contention. This enables services
// that require high-frequency concurrent cache reads in order to quickly
// deliver IPNI find results.
//
// ## All Provider Information Cached
//
// The provider cache maintains a unified view of all provider information
// across all data sources. Caching provider information in builk allows the
// cached data to be refreshed with fewer fetches over the network. This is
// particularly important when there are multiple sources to fetch provider
// information from.
//
// Long-running services that anticipate getting provider information for many
// providers should refresh the cache immediately to preload it before use.
// Short-lived use where information is fetched for only a small number of
// providers, can choose not to preload the cache and can also disable
// auto-refresh.
//
// ## Cache Refresh
//
// If the cache refresh interval is non-zero (default is 5 minutes), then a
// timer goroutine sets a flag to indicate that refresh is required. THen next
// cache lookup checks the flag and if indicated launched a goroutine to do the
// refresh and reset the timer. This way there is no need to have a background
// goroutine that need to be stopped when done with the cache, and there will
// be activity if the cache is not being actively used. A refresh can also be
// explicitly requested.
//
// Each refresh updates provider information from all sources. An an updated
// view of the cached data is built, and is then atomically set as the cache's
// read-only lock-free data.
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
// longer available from any of the sources for longer than the configured
// time-to-live. The time-to-live countdown begins when the information is seen
// to be no longer available. Negative cache entries are also evicted after
// having been in the cache for the time-to-live.
//
// ## Multiple Overlapping Data Sources
//
// The cache can be configured with multiple data sources from which provider
// information is fetched. If the same provider information is returned from
// multiple data sources, then the information with the most recent timestamp
// is used.
package pcache
