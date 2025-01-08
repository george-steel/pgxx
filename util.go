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
