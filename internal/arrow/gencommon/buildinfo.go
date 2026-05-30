package gencommon

import "runtime/debug"

// VCSRevision returns the first 8 characters of the VCS revision embedded in
// the binary's build info, or an empty string when the information is not
// available (e.g. when built with `go run` without VCS metadata).
func VCSRevision() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" && len(s.Value) >= 8 {
			return s.Value[:8]
		}
	}
	return ""
}
