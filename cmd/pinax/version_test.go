package main

import (
	"runtime"
	"strings"
	"testing"
)

func TestVersionStringWithBuildMetadata(t *testing.T) {
	oldVersion, oldCommit, oldDate := version, commit, buildDate
	t.Cleanup(func() {
		version, commit, buildDate = oldVersion, oldCommit, oldDate
	})

	version = "v1.2.3"
	commit = "abc123def456"
	buildDate = "2026-10-18T12:00:00Z"

	got := versionString()
	for _, want := range []string{
		"pinax v1.2.3",
		"commit abc123def456",
		"built 2026-10-18T12:00:00Z",
		runtime.Version(),
		runtime.GOOS + "/" + runtime.GOARCH,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("versionString() = %q, missing %q", got, want)
		}
	}
}

func TestVersionStringDevelopmentBuild(t *testing.T) {
	oldVersion, oldCommit, oldDate := version, commit, buildDate
	t.Cleanup(func() {
		version, commit, buildDate = oldVersion, oldCommit, oldDate
	})

	version = "dev"
	commit = "none"
	buildDate = "unknown"

	got := versionString()
	if !strings.HasPrefix(got, "pinax dev ") {
		t.Fatalf("versionString() = %q, want prefix %q", got, "pinax dev ")
	}
	if strings.Contains(got, "commit") || strings.Contains(got, "built") {
		t.Fatalf("versionString() = %q, development builds should not show commit/build date", got)
	}
}
