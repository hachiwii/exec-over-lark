package version

import "strings"

var Version = "dev"

func String() string {
	if strings.TrimSpace(Version) == "" {
		return "dev"
	}
	return Version
}
