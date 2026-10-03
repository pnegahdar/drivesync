//go:build darwin

package drivesync

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/ebitengine/purego"
)

// FSEvents uses one stream for the whole tree, avoiding kqueue's descriptor per file.
// Callback info contains an integer handle, never a retained Go pointer.
var macEvents struct {
	once          sync.Once
	err           error
	callback      uintptr
	channels      sync.Map
	next          atomic.Uint64
	stringCreate  func(uintptr, string, uint32) uintptr
	arrayCreate   func(uintptr, unsafe.Pointer, int64, uintptr) uintptr
	release       func(uintptr)
	getLoop       func() uintptr
	runLoop       func(uintptr, float64, bool) int32
	stopLoop      func(uintptr)
	create        func(uintptr, uintptr, unsafe.Pointer, uintptr, uint64, float64, uint32) uintptr
	schedule      func(uintptr, uintptr, uintptr)
	start         func(uintptr) bool
	stop          func(uintptr)
	invalidate    func(uintptr)
	streamRelease func(uintptr)
}

func initMacEvents() {
	macEvents.once.Do(func() {
		cf, e := purego.Dlopen("/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation", purego.RTLD_NOW|purego.RTLD_GLOBAL)
		if e != nil {
			macEvents.err = e
			return
		}
		cs, e := purego.Dlopen("/System/Library/Frameworks/CoreServices.framework/CoreServices", purego.RTLD_NOW|purego.RTLD_GLOBAL)
		if e != nil {
			macEvents.err = e
			return
		}
		bind := func(target any, lib uintptr, name string) {
			if macEvents.err != nil {
				return
			}
			ptr, e := purego.Dlsym(lib, name)
			if e != nil {
				macEvents.err = e
				return
			}
			purego.RegisterFunc(target, ptr)
		}
		bind(&macEvents.stringCreate, cf, "CFStringCreateWithCString")
		bind(&macEvents.arrayCreate, cf, "CFArrayCreate")
		bind(&macEvents.release, cf, "CFRelease")
		bind(&macEvents.getLoop, cf, "CFRunLoopGetCurrent")
		bind(&macEvents.runLoop, cf, "CFRunLoopRunInMode")
		bind(&macEvents.stopLoop, cf, "CFRunLoopStop")
		bind(&macEvents.create, cs, "FSEventStreamCreate")
		bind(&macEvents.schedule, cs, "FSEventStreamScheduleWithRunLoop")
		bind(&macEvents.start, cs, "FSEventStreamStart")
		bind(&macEvents.stop, cs, "FSEventStreamStop")
		bind(&macEvents.invalidate, cs, "FSEventStreamInvalidate")
		bind(&macEvents.streamRelease, cs, "FSEventStreamRelease")
		if macEvents.err == nil {
			macEvents.callback = purego.NewCallback(func(stream, info, num, paths, flags, ids uintptr) {
				if v, ok := macEvents.channels.Load(info); ok {
					select {
					case v.(chan<- struct{}) <- struct{}{}:
					default:
					}
				}
			})
		}
	})
}
func watchDirectory(ctx context.Context, dir string, wake chan<- struct{}) (func(), string, error) {
	initMacEvents()
	if macEvents.err != nil {
		return nil, "polling", macEvents.err
	}
	handle := uintptr(macEvents.next.Add(1))
	macEvents.channels.Store(handle, wake)
	ready := make(chan error, 1)
	done := make(chan struct{})
	var loop uintptr
	var stopping atomic.Bool
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		defer close(done)
		defer macEvents.channels.Delete(handle)
		p := macEvents.stringCreate(0, dir, 0x08000100)
		if p == 0 {
			ready <- errors.New("FSEvents path allocation failed")
			return
		}
		defer macEvents.release(p)
		values := []uintptr{p}
		array := macEvents.arrayCreate(0, unsafe.Pointer(&values[0]), 1, 0)
		runtime.KeepAlive(values)
		if array == 0 {
			ready <- errors.New("FSEvents paths allocation failed")
			return
		}
		defer macEvents.release(array)
		mode := macEvents.stringCreate(0, "kCFRunLoopDefaultMode", 0x08000100)
		defer macEvents.release(mode)
		contextValue := struct {
			version                            int64
			info, retain, release, description uintptr
		}{info: handle}
		stream := macEvents.create(0, macEvents.callback, unsafe.Pointer(&contextValue), array, ^uint64(0), 0.05, 2|4|16)
		runtime.KeepAlive(contextValue)
		if stream == 0 {
			ready <- errors.New("FSEvents stream creation failed")
			return
		}
		defer macEvents.streamRelease(stream)
		defer macEvents.invalidate(stream)
		loop = macEvents.getLoop()
		macEvents.schedule(stream, loop, mode)
		if !macEvents.start(stream) {
			ready <- errors.New("FSEvents stream start failed")
			return
		}
		defer macEvents.stop(stream)
		ready <- nil
		for !stopping.Load() {
			macEvents.runLoop(mode, 0.25, true)
		}
	}()
	if e := <-ready; e != nil {
		<-done
		return nil, "polling", e
	}
	var once sync.Once
	closeWatch := func() { once.Do(func() { stopping.Store(true); macEvents.stopLoop(loop); <-done }) }
	go func() {
		select {
		case <-ctx.Done():
			closeWatch()
		case <-done:
		}
	}()
	return closeWatch, "FSEvents", nil
}
