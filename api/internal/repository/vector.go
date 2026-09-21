package repository

import (
	"fmt"
	"strconv"
	"strings"
)

// FormatVector renders a float32 slice as a pgvector text literal, e.g.
// "[0.1,0.2,0.3]", suitable for use as a query parameter cast with `::vector`.
func FormatVector(v []float32) string {
	parts := make([]string, len(v))
	for i, f := range v {
		parts[i] = strconv.FormatFloat(float64(f), 'f', -1, 32)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

// ParseVector parses a pgvector text representation (as returned by
// `embedding::text`) back into a float64 slice.
func ParseVector(s string) ([]float64, error) {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")
	if s == "" {
		return nil, fmt.Errorf("empty vector literal")
	}
	parts := strings.Split(s, ",")
	out := make([]float64, len(parts))
	for i, p := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
		if err != nil {
			return nil, fmt.Errorf("parse vector component %q: %w", p, err)
		}
		out[i] = f
	}
	return out, nil
}

// AverageVectors computes the element-wise mean of one or more equal-length
// vectors (used to build a topic "centroid" from its representative
// messages' embeddings).
func AverageVectors(vectors [][]float64) ([]float64, error) {
	if len(vectors) == 0 {
		return nil, fmt.Errorf("no vectors to average")
	}
	dim := len(vectors[0])
	sum := make([]float64, dim)
	for _, v := range vectors {
		if len(v) != dim {
			return nil, fmt.Errorf("vector dimension mismatch: %d vs %d", len(v), dim)
		}
		for i, f := range v {
			sum[i] += f
		}
	}
	for i := range sum {
		sum[i] /= float64(len(vectors))
	}
	return sum, nil
}

// FormatVector64 renders a float64 slice (e.g. an averaged centroid) as a
// pgvector text literal.
func FormatVector64(v []float64) string {
	parts := make([]string, len(v))
	for i, f := range v {
		parts[i] = strconv.FormatFloat(f, 'f', -1, 64)
	}
	return "[" + strings.Join(parts, ",") + "]"
}
