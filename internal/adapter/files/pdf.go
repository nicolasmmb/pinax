package files

import (
	"context"
	"fmt"
	"strings"

	"github.com/nicolasmmb/pinax/internal/domain"
	pdfoxide "github.com/yfedoseev/pdf_oxide/go"
)

type PDF struct{}

func (PDF) Supports(extension string) bool { return extension == ".pdf" }

func (PDF) Extract(ctx context.Context, path string) ([]domain.Page, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	doc, err := pdfoxide.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open PDF: %w", err)
	}
	defer doc.Close()
	count, err := doc.PageCount()
	if err != nil {
		return nil, fmt.Errorf("read PDF page count: %w", err)
	}
	texts := make([]string, count)
	for i := range count {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		texts[i], err = doc.ExtractText(i)
		if err != nil {
			return nil, fmt.Errorf("extract PDF page %d: %w", i+1, err)
		}
	}
	return pdfPages(texts), nil
}

func pdfPages(texts []string) []domain.Page {
	pages := make([]domain.Page, 0, len(texts))
	for i, text := range texts {
		text = strings.TrimSpace(text)
		if text != "" {
			pages = append(pages, domain.Page{Number: i + 1, Text: text})
		}
	}
	return pages
}
