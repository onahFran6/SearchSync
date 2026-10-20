package main

import "testing"

func TestParseFlags_DryRunDefaultsTrue(t *testing.T) {
	cfg, err := parseFlags([]string{"-target=indexer", "-from-offset=0", "-partition=0"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cfg.dryRun {
		t.Fatal("dry-run must default to true so a replay run is safe by default")
	}
}

func TestParseFlags_DryRunCanBeDisabledExplicitly(t *testing.T) {
	cfg, err := parseFlags([]string{"-target=indexer", "-from-offset=0", "-partition=0", "-dry-run=false"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.dryRun {
		t.Fatal("expected dry-run=false to take effect")
	}
}

func TestParseFlags_RequiresKnownTarget(t *testing.T) {
	_, err := parseFlags([]string{"-target=bogus", "-from-offset=0", "-partition=0"})
	if err == nil {
		t.Fatal("expected error for unknown target")
	}
}

func TestParseFlags_RequiresExactlyOneRangeMode(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"neither range given", []string{"-target=indexer"}},
		{"both ranges given", []string{"-target=indexer", "-from-offset=0", "-partition=0", "-from-time=2026-01-01T00:00:00Z"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseFlags(tt.args); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestParseFlags_FromOffsetRequiresPartition(t *testing.T) {
	_, err := parseFlags([]string{"-target=indexer", "-from-offset=0"})
	if err == nil {
		t.Fatal("expected error when -from-offset is given without -partition")
	}
}

func TestParseFlags_DefaultsTopicPerTarget(t *testing.T) {
	cfg, err := parseFlags([]string{"-target=notifications", "-from-time=2026-01-01T00:00:00Z"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.topic == "" {
		t.Fatal("expected a default topic to be resolved for the notifications target")
	}
}
