package libipni

import (
	_ "embed"
	"encoding/json"
	"runtime/debug"
)

var (
	// Release is the release version tag value, e.g. "v1.2.3"
	Release string
	// Revision is the git commit hash.
	Revision string
	// Version is the full version string: Release-Revision.
	Version string
)

//go:embed version.json
var versionJSON []byte

func init() {
	// Read version from embedded JSON file.
	var verMap map[string]string
	json.Unmarshal(versionJSON, &verMap)
	Release = verMap["version"]

	// If running from a module, try to get the build info.
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}

	// Append the revision to the version.
	for i := range bi.Settings {
		if bi.Settings[i].Key == "vcs.revision" {
			Revision = bi.Settings[i].Value
			Version = Release + "-" + Revision
			break
		}
	}
}
