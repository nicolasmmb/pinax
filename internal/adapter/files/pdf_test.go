package files

import "testing"

func TestPDFPagesPreservesPageNumbersAndSkipsEmptyPages(t *testing.T) {
	pages := pdfPages([]string{" primeira página ", "", " terceira página "})
	if len(pages) != 2 || pages[0].Number != 1 || pages[1].Number != 3 {
		t.Fatalf("unexpected pages: %#v", pages)
	}
}
