// Package maurl converts between multiaddrs and URLs.
//
// Importing this package registers a new multiaddr protocol named "httppath"
// that allows the path part of a URL to be encoded into a multiaddr as a
// url.PathEscape encoded string.
//
// Representing a URL as a mutliaddr may be necessary when announcing content
// advertisement publishers that publish via HTTP, or when advertising content
// providers that have HTTP addresses.
package maurl
