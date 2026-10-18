package cli

import (
	"io"
	"strings"
	"testing"
)

func TestParseCommands(t *testing.T) {
	tests := []struct {
		args    []string
		name    Name
		wantErr bool
	}{
		{args: []string{"serve"}, name: Serve},
		{args: []string{"worker", "--once"}, name: Worker},
		{args: []string{"status", "--watch", "--limit", "4", "abc"}, name: Status},
		{args: []string{"index", "/docs"}, name: Index},
		{args: []string{"--version"}, name: Version},
		{args: []string{"-version"}, name: Version},
		{args: []string{"version"}, name: Version},
		{args: []string{"--version", "extra"}, wantErr: true},
		{args: []string{"index"}, wantErr: true},
		{args: []string{"serve", "extra"}, wantErr: true},
		{args: []string{"worker", "extra"}, wantErr: true},
		{args: []string{"status", "one", "two"}, wantErr: true},
		{args: []string{"unknown"}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(strings.Join(test.args, "_"), func(t *testing.T) {
			cmd, err := Parse(test.args, io.Discard)
			if test.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cmd.Name != test.name {
				t.Fatalf("command = %q, want %q", cmd.Name, test.name)
			}
		})
	}
}

func TestParseStatusOptions(t *testing.T) {
	cmd, err := Parse([]string{"status", "--watch", "--limit", "4", "abc"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cmd.StatusJob != "abc" || !cmd.Watch || cmd.Limit != 4 {
		t.Fatalf("status command = %#v", cmd)
	}
}

func TestParseWorkerOnce(t *testing.T) {
	cmd, err := Parse([]string{"worker", "--once"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Name != Worker || !cmd.Once {
		t.Fatalf("worker command = %#v", cmd)
	}
}
