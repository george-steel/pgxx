// Copyright 2024-2025 George Steel
// SPDX-License-Identifier: MIT

package pgxx

// Helper function to return the first item of a list, or nil if empty
func Head[T any](xs []T) *T {
	if len(xs) == 0 {
		return nil
	} else {
		return &xs[0]
	}
}

// Safe dereferencing of a pointer with fallback.
func OrZero[T any](p *T) T {
	if p == nil {
		var zero T
		return zero
	} else {
		return *p
	}
}

// Function to generate a pointer to a constant calue inside an expression.
// Useful for nullable fields in struct literals.
func NotNil[T any](x T) *T {
	return &x
}
