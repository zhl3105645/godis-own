package utils

import "os"

// Equals check whether the given value is equal
func Equals(a, b interface{}) bool {
	sliceA, okA := a.([]byte)
	sliceB, okB := b.([]byte)
	if okA && okB {
		return BytesEquals(sliceA, sliceB)
	}
	return a == b
}

// BytesEquals check whether the given bytes is equal
func BytesEquals(a, b []byte) bool {
	if (a == nil && b != nil) || (a != nil && b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	size := len(a)
	for i := 0; i < size; i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ToCmdLine convert strings to [][]byte
func ToCmdLine(cmd ...string) [][]byte {
	args := make([][]byte, len(cmd))
	for i, s := range cmd {
		args[i] = []byte(s)
	}
	return args
}

// ToCmdLine2 convert commandName and string-type argument to [][]byte
func ToCmdLine2(commandName string, args ...string) [][]byte {
	result := make([][]byte, len(args)+1)
	result[0] = []byte(commandName)
	for i, s := range args {
		result[i+1] = []byte(s)
	}
	return result
}

// ToCmdLine3 convert commandName and []byte-type argument to CmdLine
func ToCmdLine3(commandName string, args ...[]byte) [][]byte {
	result := make([][]byte, len(args)+1)
	result[0] = []byte(commandName)
	for i, s := range args {
		result[i+1] = s
	}
	return result
}

func FileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}
