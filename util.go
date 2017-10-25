package mongonet

import "fmt"
import "runtime"
import "strings"

// ---
const _64bit = 1 << (^uintptr(0) >> 63) / 2

type StackError struct {
	Message    string
	Stacktrace []string
}

func NewStackErrorf(messageFmt string, args ...interface{}) *StackError {
	return &StackError{
		Message:    fmt.Sprintf(messageFmt, args...),
		Stacktrace: stacktrace(),
	}
}

func (self *StackError) Error() string {
	return fmt.Sprintf("%s\n\t%s", self.Message, strings.Join(self.Stacktrace, "\n\t"))
}

func stacktrace() []string {
	ret := make([]string, 0, 2)
	for skip := 2; true; skip++ {
		_, file, line, ok := runtime.Caller(skip)
		if ok == false {
			break
		}

		ret = append(ret, fmt.Sprintf("at %s:%d", stripDirectories(file, 2), line))
	}

	return ret
}

func stripDirectories(filepath string, toKeep int) string {
	var idxCutoff int
	if idxCutoff = strings.LastIndex(filepath, "/"); idxCutoff == -1 {
		return filepath
	}

	for dirToKeep := 0; dirToKeep < toKeep; dirToKeep++ {
		switch idx := strings.LastIndex(filepath[:idxCutoff], "/"); idx {
		case -1:
			break
		default:
			idxCutoff = idx
		}
	}

	return filepath[idxCutoff+1:]
}

// Checks adapted from go 1.6 check in makeslice() (https://github.com/golang/go/blob/release-branch.go1.6/src/runtime/slice.go#L25)
func SafeMakeByteSlice(length int32) ([]byte, error) {
	// _64bit = 1 on 64-bit systems, 0 on 32-bit systems
	goosWindows, goosDarwin, goarchArm64 := 0, 0, 0
	if runtime.GOOS == "windows" {
		goosWindows = 1
	}
	if runtime.GOOS == "darwin" {
		goosDarwin = 1
	}
	if runtime.GOARCH == "arm64" {
		goarchArm64 = 1
	}
	var _MHeapMap_TotalBits = (_64bit*goosWindows)*35 + (_64bit*(1-goosWindows)*(1-goosDarwin*goarchArm64))*39 + goosDarwin*goarchArm64*31 + (1-_64bit)*32
	var _MaxMem = uintptr(1<<uint64(_MHeapMap_TotalBits) - 1)

	if length < 0 || uintptr(length) > _MaxMem {
		return nil, NewStackErrorf("byte slice len has invalid size (%v). MaxMem=%v", length, _MaxMem)
	}

	buf := make([]byte, length)
	return buf, nil
}
