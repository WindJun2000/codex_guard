package main

/*
#include <stdint.h>
#include <stdlib.h>

typedef struct {
	void* ptr;
	size_t len;
} cliproxy_buffer;

typedef int (*cliproxy_host_call_fn)(void*, const char*, const uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_host_free_fn)(void*, size_t);

typedef struct {
	uint32_t abi_version;
	void* host_ctx;
	cliproxy_host_call_fn call;
	cliproxy_host_free_fn free_buffer;
} cliproxy_host_api;

typedef int (*cliproxy_plugin_call_fn)(char*, uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_plugin_free_fn)(void*, size_t);
typedef void (*cliproxy_plugin_shutdown_fn)(void);

typedef struct {
	uint32_t abi_version;
	cliproxy_plugin_call_fn call;
	cliproxy_plugin_free_fn free_buffer;
	cliproxy_plugin_shutdown_fn shutdown;
} cliproxy_plugin_api;

extern int cliproxyPluginCall(char*, uint8_t*, size_t, cliproxy_buffer*);
extern void cliproxyPluginFree(void*, size_t);
extern void cliproxyPluginShutdown(void);

static int call_host_api(cliproxy_host_api* host, const char* method, const uint8_t* request, size_t request_len, cliproxy_buffer* response) {
	if (host == NULL || host->call == NULL) {
		return 1;
	}
	return host->call(host->host_ctx, method, request, request_len, response);
}

static void free_host_buffer(cliproxy_host_api* host, void* ptr, size_t len) {
	if (host != NULL && host->free_buffer != NULL && ptr != NULL) {
		host->free_buffer(ptr, len);
	}
}
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
)

// hostAPI holds the host callbacks provided at plugin init.
var hostAPI atomic.Pointer[C.cliproxy_host_api]

func main() {}

//export cliproxy_plugin_init
func cliproxy_plugin_init(host *C.cliproxy_host_api, plugin *C.cliproxy_plugin_api) C.int {
	if plugin == nil {
		return 1
	}
	hostAPI.Store(host)
	plugin.abi_version = C.uint32_t(pluginabi.ABIVersion)
	plugin.call = C.cliproxy_plugin_call_fn(C.cliproxyPluginCall)
	plugin.free_buffer = C.cliproxy_plugin_free_fn(C.cliproxyPluginFree)
	plugin.shutdown = C.cliproxy_plugin_shutdown_fn(C.cliproxyPluginShutdown)
	return 0
}

//export cliproxyPluginCall
func cliproxyPluginCall(method *C.char, request *C.uint8_t, requestLen C.size_t, response *C.cliproxy_buffer) C.int {
	if response != nil {
		response.ptr = nil
		response.len = 0
	}
	if method == nil {
		writeResponse(response, errorEnvelope("invalid_method", "method is required"))
		return 1
	}

	var requestBytes []byte
	if request != nil && requestLen > 0 {
		requestBytes = C.GoBytes(unsafe.Pointer(request), C.int(requestLen))
	}

	raw, errHandle := handleMethod(C.GoString(method), requestBytes)
	if errHandle != nil {
		writeResponse(response, errorEnvelope("plugin_error", errHandle.Error()))
		return 1
	}
	writeResponse(response, raw)
	return 0
}

//export cliproxyPluginFree
func cliproxyPluginFree(ptr unsafe.Pointer, len C.size_t) {
	if ptr != nil {
		C.free(ptr)
	}
}

//export cliproxyPluginShutdown
func cliproxyPluginShutdown() {
	globalRefresher.stopRefresher()
	stateMu.Lock()
	globalAccountStates = map[string]*AccountState{}
	stateMu.Unlock()
}

func writeResponse(response *C.cliproxy_buffer, raw []byte) {
	if response == nil || len(raw) == 0 {
		return
	}
	ptr := C.CBytes(raw)
	if ptr == nil {
		return
	}
	response.ptr = ptr
	response.len = C.size_t(len(raw))
}

// callHostRaw performs one plugin-to-host ABI call and returns the raw
// response bytes. It is the single transport used for host.auth.* and host.log.
func callHostRaw(method string, payload []byte) ([]byte, error) {
	host := hostAPI.Load()
	if host == nil {
		return nil, fmt.Errorf("host callback %s unavailable", method)
	}

	cMethod := C.CString(method)
	defer C.free(unsafe.Pointer(cMethod))

	var response C.cliproxy_buffer
	var requestPtr *C.uint8_t
	if len(payload) > 0 {
		cPayload := C.CBytes(payload)
		if cPayload == nil {
			return nil, fmt.Errorf("allocate host callback payload %s", method)
		}
		defer C.free(cPayload)
		requestPtr = (*C.uint8_t)(cPayload)
	}

	code := C.call_host_api(host, cMethod, requestPtr, C.size_t(len(payload)), &response)
	var raw []byte
	if response.ptr != nil && response.len > 0 {
		raw = C.GoBytes(response.ptr, C.int(response.len))
	}
	if response.ptr != nil {
		C.free_host_buffer(host, response.ptr, response.len)
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("host callback %s returned no response, code=%d", method, int(code))
	}
	return raw, nil
}

// callHostJSON marshals a request and unmarshals the host response.
func callHostJSON(method string, request any, response any) error {
	payload, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("marshal %s request: %w", method, err)
	}
	raw, err := callHostRaw(method, payload)
	if err != nil {
		return err
	}
	// The host wraps responses in pluginabi.Envelope.
	var env pluginabi.Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("decode %s envelope: %w", method, err)
	}
	if !env.OK {
		if env.Error != nil {
			return fmt.Errorf("%s failed: %s (%s)", method, env.Error.Message, env.Error.Code)
		}
		return fmt.Errorf("%s failed", method)
	}
	if response != nil && len(env.Result) > 0 {
		if err := json.Unmarshal(env.Result, response); err != nil {
			return fmt.Errorf("decode %s result: %w", method, err)
		}
	}
	return nil
}

// logf emits a host.log entry with printf-style formatting.
func logf(level, format string, args ...any) {
	payload, _ := json.Marshal(map[string]any{"level": level, "message": fmt.Sprintf(format, args...)})
	if _, err := callHostRaw(pluginabi.MethodHostLog, payload); err != nil {
		// Logging must never break plugin operation.
		return
	}
}
