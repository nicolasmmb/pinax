package main

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// These values are overridden at build time via -ldflags, e.g.:
//
//	go build -ldflags "-X main.version=v0.1.0 -X main.commit=abc123 -X main.buildDate=2026-10-18T12:00:00Z"
var (
	version   = "dev"
	commit    = "none"
	buildDate = "unknown"
)

// applyBuildInfo fills version and VCS metadata when the binary was installed
// through the Go module proxy (go install ...@v0.1.0), where the release
// workflow's -ldflags are not available.
func init() {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	if version == "dev" && info.Main.Version != "" && info.Main.Version != "(devel)" {
		version = info.Main.Version
	}
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			if commit == "none" && setting.Value != "" {
				commit = setting.Value
				if len(commit) > 12 {
					commit = commit[:12]
				}
			}
		case "vcs.time":
			if buildDate == "unknown" && setting.Value != "" {
				buildDate = setting.Value
			}
		}
	}
}

func versionString() string {
	line := fmt.Sprintf("pinax %s", version)
	if commit != "" && commit != "none" {
		line += fmt.Sprintf(" (commit %s", commit)
		if buildDate != "" && buildDate != "unknown" {
			line += fmt.Sprintf(", built %s", buildDate)
		}
		line += ")"
	}
	return fmt.Sprintf("%s %s %s/%s", line, runtime.Version(), runtime.GOOS, runtime.GOARCH)
}
