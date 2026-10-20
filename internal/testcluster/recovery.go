package testcluster

import (
	"io"
	"net"
	"os"
	"reflect"
	"syscall"
)

// IsRecoveryError identifies expected transport failures in development tests.
// Callers may additionally allow exact context or session sentinels according
// to their test's policy. Every joined or wrapped cause must be allowed; an
// uncertain command outcome alone never makes a failure acceptable.
func IsRecoveryError(err error, allowed ...error) bool {
	return recoveryError(err, allowed, false, 0)
}

func recoveryError(err error, allowed []error, network bool, depth int) bool {
	if err == nil || depth >= 64 {
		return false
	}
	value := reflect.ValueOf(err)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if value.IsNil() {
			return false
		}
	}
	switch e := err.(type) {
	case *net.OpError:
		return recoveryError(e.Err, allowed, true, depth+1)
	case interface{ Unwrap() []error }:
		children := e.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !recoveryError(child, allowed, network, depth+1) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		if child := e.Unwrap(); child != nil {
			return recoveryError(child, allowed, network, depth+1)
		}
	}
	if value.Type().Comparable() {
		if err == io.EOF || err == io.ErrUnexpectedEOF || err == net.ErrClosed {
			return true
		}
		for _, sentinel := range allowed {
			if err == sentinel {
				return true
			}
		}
		if network && err == os.ErrDeadlineExceeded {
			return true
		}
	}
	if network {
		switch e := err.(type) {
		case syscall.Errno:
			return e != 0
		case *net.DNSError:
			return e.Err != ""
		}
	}
	return false
}
