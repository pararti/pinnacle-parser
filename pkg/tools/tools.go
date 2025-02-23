package tools

import "math"

func ConversionOddFromUSA(odd int) float64 {
	var result float64
	if odd > 0 {
		result = (float64(odd) / 100) + 1
	} else if odd == 0 {
		return 0.0
	} else {
		result = (100 / float64(odd)) + 1
	}

	return RoundToFixed(result, 2)
}

func RoundToFixed(num float64, precision int) float64 {
	output := math.Pow(10, float64(precision))
	return float64(round(num*output)) / output
}
func round(num float64) int {
	return int(num + math.Copysign(0.5, num))
}
