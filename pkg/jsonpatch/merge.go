package jsonpatch

import (
	"reflect"

	"github.com/bytedance/sonic"
	"github.com/bytedance/sonic/option"
)

var (
	// Use the fastest configuration for better performance
	sonicAPI = sonic.ConfigFastest

	// Record types we've already precompiled
	precompiledTypes = make(map[reflect.Type]bool)
)

// pretouchType precompiles the reflection-based encoder/decoder for type t
func pretouchType(t reflect.Type) {
	// Skip if already precompiled or if running on a non-amd64 architecture
	if precompiledTypes[t] {
		return
	}

	// Add to precompiled map to avoid redundant precompilation
	precompiledTypes[t] = true

	// Use Sonic's Pretouch to compile the encoder/decoder ahead of time
	// This helps reduce first-time encoding/decoding latency
	sonic.Pretouch(t, option.WithCompileRecursiveDepth(10))
}

// ApplyMergePatch applies an RFC7396 merge patch to an original object
func ApplyMergePatch(original, patch interface{}) (interface{}, error) {
	// Pretouch types for better performance
	pretouchType(reflect.TypeOf(original))
	pretouchType(reflect.TypeOf(patch))

	// Convert both to JSON
	originalJSON, err := sonicAPI.Marshal(original)
	if err != nil {
		return nil, err
	}

	patchJSON, err := sonicAPI.Marshal(patch)
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
	if err := sonicAPI.Unmarshal(resultJSON, resultValue); err != nil {
		return nil, err
	}

	return resultValue, nil
}

// applyMergePatchJSON implements RFC7396 merge logic at the JSON level
func applyMergePatchJSON(original, patch []byte) ([]byte, error) {
	// Parse both JSON documents
	var originalMap map[string]interface{}
	var patchMap map[string]interface{}

	// Pre-allocate map capacity for originalMap based on JSON size heuristic
	// This can reduce allocations during unmarshaling
	originalMapSize := estimateMapSize(len(original))
	originalMap = make(map[string]interface{}, originalMapSize)

	if err := sonicAPI.Unmarshal(original, &originalMap); err != nil {
		return nil, err
	}

	if err := sonicAPI.Unmarshal(patch, &patchMap); err != nil {
		return nil, err
	}

	// Apply merge logic recursively
	result := mergeMaps(originalMap, patchMap)

	// Convert back to JSON
	return sonicAPI.Marshal(result)
}

// estimateMapSize estimates the number of top-level keys in a JSON object
// based on its byte length. This is a heuristic to optimize map allocation.
func estimateMapSize(jsonLen int) int {
	// Very rough heuristic: assume average key-value pair is about 30 bytes
	estimatedPairs := jsonLen / 30

	// Ensure reasonable minimum and maximum
	if estimatedPairs < 8 {
		return 8 // Minimum pre-allocation
	}
	if estimatedPairs > 1024 {
		return 1024 // Maximum pre-allocation
	}
	return estimatedPairs
}

// mergeMaps implements the RFC7396 merge logic for maps
func mergeMaps(original, patch map[string]interface{}) map[string]interface{} {
	// Pre-allocate the result map with capacity for all original fields
	// This reduces map growth and reallocation during merging
	result := make(map[string]interface{}, len(original))

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
				// Pre-allocate nested map with appropriate capacity
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
