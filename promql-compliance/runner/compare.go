package runner

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// CompareResponses first provides strict structural parity. Numeric tolerance
// refinement is kept at this seam so transport and lifecycle code stay simple.
func CompareResponses(reference, test QueryResponse) error {
	if reference.Status != test.Status {
		return fmt.Errorf("status differs: reference=%q test=%q", reference.Status, test.Status)
	}
	if reference.ErrorType != test.ErrorType || reference.Error != test.Error {
		return fmt.Errorf("query errors differ: reference=%q/%q test=%q/%q", reference.ErrorType, reference.Error, test.ErrorType, test.Error)
	}
	var left, right any
	if err := json.Unmarshal(reference.Data, &left); err != nil {
		return fmt.Errorf("decode reference data: %w", err)
	}
	if err := json.Unmarshal(test.Data, &right); err != nil {
		return fmt.Errorf("decode test data: %w", err)
	}
	leftJSON, _ := json.Marshal(left)
	rightJSON, _ := json.Marshal(right)
	if !bytes.Equal(leftJSON, rightJSON) {
		return fmt.Errorf("query data differs: reference=%s test=%s", leftJSON, rightJSON)
	}
	return nil
}
