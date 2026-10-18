package files

import (
	"context"
	"os"

	"github.com/nicolasmmb/pinax/internal/domain"
)

type Markdown struct{}

func (Markdown) Supports(extension string) bool { return extension == ".md" }

func (Markdown) Extract(_ context.Context, path string) ([]domain.Page, error) {
	text, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return []domain.Page{{Number: 1, Text: string(text)}}, nil
}
