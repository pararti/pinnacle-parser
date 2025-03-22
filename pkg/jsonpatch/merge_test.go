package jsonpatch

import (
	"encoding/json"
	"reflect"
	"testing"
)

type TestStruct struct {
	ID     int           `json:"id"`
	Name   string        `json:"name,omitempty"`
	Value  int           `json:"value,omitempty"`
	Nested *NestedStruct `json:"nested,omitempty"`
}

type NestedStruct struct {
	ID     int    `json:"id"`
	Name   string `json:"name,omitempty"`
	Detail string `json:"detail,omitempty"`
}

func TestApplyMergePatch(t *testing.T) {
	tests := []struct {
		name     string
		original interface{}
		patch    interface{}
		expected interface{}
	}{
		{
			name: "Basic patch",
			original: &TestStruct{
				ID:    1,
				Name:  "Original",
				Value: 100,
			},
			patch: &TestStruct{
				ID:   1,
				Name: "Updated",
			},
			expected: &TestStruct{
				ID:    1,
				Name:  "Updated",
				Value: 100, // Preserved from original
			},
		},
		{
			name: "Nested structure",
			original: &TestStruct{
				ID:    1,
				Name:  "Original",
				Value: 100,
				Nested: &NestedStruct{
					ID:     10,
					Name:   "Original Nested",
					Detail: "Original Detail",
				},
			},
			patch: &TestStruct{
				ID: 1,
				Nested: &NestedStruct{
					ID:   10,
					Name: "Updated Nested",
				},
			},
			expected: &TestStruct{
				ID:    1,
				Name:  "Original", // Preserved from original
				Value: 100,        // Preserved from original
				Nested: &NestedStruct{
					ID:     10,
					Name:   "Updated Nested",
					Detail: "Original Detail", // Preserved from original
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ApplyMergePatch(tt.original, tt.patch)
			if err != nil {
				t.Errorf("ApplyMergePatch failed: %v", err)
				return
			}

			// Convert to JSON and back for easy comparison
			expectedJSON, _ := json.Marshal(tt.expected)
			resultJSON, _ := json.Marshal(result)

			var expectedMap map[string]interface{}
			var resultMap map[string]interface{}
			json.Unmarshal(expectedJSON, &expectedMap)
			json.Unmarshal(resultJSON, &resultMap)

			if !reflect.DeepEqual(resultMap, expectedMap) {
				t.Errorf("Result doesn't match expected:\nExpected: %+v\nGot: %+v", expectedMap, resultMap)
			}
		})
	}
}
