package commanders

// This global var GpdbVersion should have a value set at build time.
// see Makefile for -ldflags "-X etc"
var GpdbVersion = ""

type VersionCommand struct{}

const DefaultGpdbVersion = "gp_upgrade unknown version"

func VersionString() string {
	if GpdbVersion == "" {
		return DefaultGpdbVersion
	}
	return "gp_upgrade version " + GpdbVersion
}
