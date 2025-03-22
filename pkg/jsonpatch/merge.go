package jsonpatch

import (
	"encoding/json"
	"reflect"
)

// ApplyMergePatch applies an RFC7396 merge patch to an original object
func ApplyMergePatch(original, patch interface{}) (interface{}, error) {
	// Convert both to JSON
	originalJSON, err := json.Marshal(original)
	if err != nil {
		return nil, err
	}

	patchJSON, err := json.Marshal(patch)
	if err != nil {
		return nil, err
	}

	// Apply JSON merge patch
	resultJSON, err := applyMergePatchJSON(originalJSON, patchJSON)
	if err != nil {
		return nil, err
	}

	// Get the correct type for the result
	originalType := reflect.TypeOf(original)
	var resultValue interface{}

	// Check if original is a pointer and handle accordingly
	if originalType.Kind() == reflect.Ptr {
		// Create a new instance of the pointed-to type
		resultValue = reflect.New(originalType.Elem()).Interface()
	} else {
		// Create a new instance of the non-pointer type
		resultValue = reflect.New(originalType).Interface()
	}

	// Unmarshal back into the result
	if err := json.Unmarshal(resultJSON, resultValue); err != nil {
		return nil, err
	}

	return resultValue, nil
}

// applyMergePatchJSON implements RFC7396 merge logic at the JSON level
func applyMergePatchJSON(original, patch []byte) ([]byte, error) {
	// Parse both JSON documents
	var originalMap map[string]interface{}
	var patchMap map[string]interface{}

	if err := json.Unmarshal(original, &originalMap); err != nil {
		return nil, err
	}

	if err := json.Unmarshal(patch, &patchMap); err != nil {
		return nil, err
	}

	// Apply merge logic recursively
	result := mergeMaps(originalMap, patchMap)

	// Convert back to JSON
	return json.Marshal(result)
}

// mergeMaps implements the RFC7396 merge logic for maps
func mergeMaps(original, patch map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	// Copy all fields from original
	for k, v := range original {
		result[k] = v
	}

	// Apply patch fields
	for k, v := range patch {
		if v == nil {
			// Remove field if value is null
			delete(result, k)
		} else if originalValue, exists := original[k]; exists {
			// If field exists in both and both are objects, merge recursively
			originalMap, originalIsMap := originalValue.(map[string]interface{})
			patchMap, patchIsMap := v.(map[string]interface{})

			if originalIsMap && patchIsMap {
				result[k] = mergeMaps(originalMap, patchMap)
			} else {
				// Otherwise replace with patch value
				result[k] = v
			}
		} else {
			// If field doesn't exist in original, add it
			result[k] = v
		}
	}

	return result
}
