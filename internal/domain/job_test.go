package domain

import "testing"

func TestJobStatusClassification(t *testing.T) {
	for _, status := range []JobStatus{JobQueued, JobRunning} {
		if !status.Valid() || status.Terminal() || !status.Active() {
			t.Fatalf("%s should be active", status)
		}
	}
	for _, status := range []JobStatus{JobCompleted, JobCompletedWithErrors, JobCanceled, JobFailed} {
		if !status.Valid() || !status.Terminal() || status.Active() {
			t.Fatalf("%s should be terminal", status)
		}
	}
	if JobStatus("bogus").Valid() {
		t.Fatal("invalid status accepted")
	}
}

func TestJobProgressPercent(t *testing.T) {
	progress := JobProgress{FilesTotal: 4, FilesDone: 1}
	if progress.Percent != 0 {
		t.Fatalf("percent = %v, want 0 before computation", progress.Percent)
	}
	if !(JobProgress{}).Complete() {
		t.Fatal("empty job should be complete")
	}
	if (JobProgress{FilesTotal: 2, FilesDone: 1}).Complete() {
		t.Fatal("partially done job should not be complete")
	}
}

func TestEnqueueRequestValidation(t *testing.T) {
	valid := EnqueueRequest{
		InputPath:      "/docs",
		InputKind:      InputDir,
		EmbeddingModel: "model",
		ChunkerVersion: "v2",
		ChunkSize:      1200,
		ChunkOverlap:   150,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	invalid := valid
	invalid.ChunkOverlap = invalid.ChunkSize
	if err := invalid.Validate(); err == nil {
		t.Fatal("invalid chunk overlap accepted")
	}
	invalid = valid
	invalid.InputKind = "bogus"
	if err := invalid.Validate(); err == nil {
		t.Fatal("invalid input kind accepted")
	}
}
