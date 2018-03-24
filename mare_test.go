package mare

import (
	"testing"
)

func TestMareWithSimpleWordCount(t *testing.T) {
	items := []interface{}{"three", "three", "three", "one"}

	result := MaRe().Log(true).InSlice(items).Map(func(input interface{}) []MapOutput {
		return []MapOutput{{Key: input, Value: 1}}
	}).Reduce(func(a, b interface{}) interface{} {
		return a.(int) + b.(int)
	})

	if len(result) != 2 ||
		result["three"] != 3 ||
		result["one"] != 1 {
		t.Fatalf("Map isn't as expected: len=%d, result 1: %v, result 2: %v !",
			len(result),
			result["three"],
			result["one"])
	}
}

func TestMareWithCountDigits(t *testing.T) {
	inputChan := make(chan interface{})
	number := 1234123
	go func() {
		defer close(inputChan)
		for {
			digit := number % 10
			number /= 10
			inputChan <- digit
			if number == 0 {
				break
			}
		}
	}()

	result := MaRe().MapWorker(2).Log(true).InChannel(inputChan).Map(func(input interface{}) []MapOutput {
		return []MapOutput{{Key: input, Value: 1}}
	}).Reduce(func(a, b interface{}) interface{} {
		return a.(int) + b.(int)
	})

	if len(result) != 4 ||
		result[1] != 2 ||
		result[2] != 2 ||
		result[3] != 2 ||
		result[4] != 1 {
		t.Fatalf("Failed digit count: actual map = %v", result)
	}

}
