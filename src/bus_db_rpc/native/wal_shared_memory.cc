#include <napi.h>
#include <windows.h>
#include <atomic>
#include <cstdint>
#include <string>

static constexpr uint32_t RING_HEADER_BYTES = 64;
static constexpr uint32_t RING_WRITE = 0;
static constexpr uint32_t RING_READ = 1;
static constexpr uint32_t RING_CAPACITY = 2;
static constexpr uint32_t RING_WRAP = 0xFFFFFFFFu;

struct RingHeader {
  std::atomic<uint32_t> write;
  std::atomic<uint32_t> read;
  std::atomic<uint32_t> capacity;
  std::atomic<uint32_t> reserved;
};

struct RingSegment {
  void* view;
  RingHeader* header;
  uint8_t* payload;
  uint32_t capacity;
  HANDLE event;
};

static uint32_t Align4(uint32_t value) {
  return (value + 3u) & ~3u;
}

static bool WriteFrame(RingSegment* ring, const uint8_t* data, uint32_t length) {
  const uint32_t footprint = Align4(4 + length);
  if (footprint >= ring->capacity) return false;

  uint32_t write = ring->header->write.load(std::memory_order_relaxed);
  uint32_t read = ring->header->read.load(std::memory_order_acquire);

  uint32_t target = write;
  const uint32_t tail = ring->capacity - write;

  if (read == write) {
    target = 0;
  } else if (write > read) {
    if (footprint > tail) {
      if (footprint >= read) return false;
      *reinterpret_cast<uint32_t*>(ring->payload + write) = RING_WRAP;
      target = 0;
    } else if (footprint == tail && read == 0) {
      return false;
    }
  } else {
    if (footprint >= read - write) return false;
  }

  *reinterpret_cast<uint32_t*>(ring->payload + target) = length;
  if (length > 0) memcpy(ring->payload + target + 4, data, length);

  ring->header->write.store((target + footprint) % ring->capacity, std::memory_order_release);
  SetEvent(ring->event);
  return true;
}

static bool ReadFrame(RingSegment* ring, uint8_t** data, uint32_t* length, uint32_t* nextRead) {
  uint32_t read = ring->header->read.load(std::memory_order_relaxed);
  const uint32_t write = ring->header->write.load(std::memory_order_acquire);
  if (read == write) return false;

  uint32_t headerValue = *reinterpret_cast<uint32_t*>(ring->payload + read);

  if (headerValue == RING_WRAP) {
    read = 0;
    if (read == write) return false;
    headerValue = *reinterpret_cast<uint32_t*>(ring->payload + read);
  }

  *length = headerValue;
  *data = ring->payload + read + 4;
  *nextRead = (read + Align4(4 + headerValue)) % ring->capacity;
  return true;
}

class SharedRing {
public:
  SharedRing(RingSegment* request, RingSegment* response)
    : request(request), response(response) {}

  RingSegment* request;
  RingSegment* response;
};

Napi::Value CreateSegment(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  if (info.Length() < 5) {
    Napi::TypeError::New(env, "create(name, requestBytes, responseBytes, overflowBytes, maxPayloadBytes)").ThrowAsJavaScriptException();
    return env.Undefined();
  }

  const std::string name = info[0].As<Napi::String>().Utf8Value();
  const uint32_t requestBytes = info[1].As<Napi::Number>().Uint32Value();
  const uint32_t responseBytes = info[2].As<Napi::Number>().Uint32Value();
  const uint32_t overflowBytes = info[3].As<Napi::Number>().Uint32Value();
  const uint32_t maxPayloadBytes = info[4].As<Napi::Number>().Uint32Value();

  const std::string requestName = name + "_REQUEST";
  const std::string responseName = name + "_RESPONSE";
  const std::string requestEventName = name + "_REQUEST_EVENT";
  const std::string responseEventName = name + "_RESPONSE_EVENT";

  HANDLE requestMapping = CreateFileMappingA(INVALID_HANDLE_VALUE, nullptr, PAGE_READWRITE, 0, requestBytes, requestName.c_str());
  HANDLE responseMapping = CreateFileMappingA(INVALID_HANDLE_VALUE, nullptr, PAGE_READWRITE, 0, responseBytes, responseName.c_str());

  if (!requestMapping || !responseMapping) {
    Napi::Error::New(env, "CreateFileMapping failed").ThrowAsJavaScriptException();
    return env.Undefined();
  }

  void* requestView = MapViewOfFile(requestMapping, FILE_MAP_ALL_ACCESS, 0, 0, requestBytes);
  void* responseView = MapViewOfFile(responseMapping, FILE_MAP_ALL_ACCESS, 0, 0, responseBytes);

  if (!requestView || !responseView) {
    Napi::Error::New(env, "MapViewOfFile failed").ThrowAsJavaScriptException();
    return env.Undefined();
  }

  HANDLE requestEvent = CreateEventA(nullptr, FALSE, FALSE, requestEventName.c_str());
  HANDLE responseEvent = CreateEventA(nullptr, FALSE, FALSE, responseEventName.c_str());

  if (!requestEvent || !responseEvent) {
    Napi::Error::New(env, "CreateEvent failed").ThrowAsJavaScriptException();
    return env.Undefined();
  }

  auto requestHeader = static_cast<RingHeader*>(requestView);
  auto responseHeader = static_cast<RingHeader*>(responseView);

  requestHeader->write.store(0);
  requestHeader->read.store(0);
  requestHeader->capacity.store(requestBytes - RING_HEADER_BYTES);

  responseHeader->write.store(0);
  responseHeader->read.store(0);
  responseHeader->capacity.store(responseBytes - RING_HEADER_BYTES);

  auto requestSegment = new RingSegment{
    requestView,
    requestHeader,
    static_cast<uint8_t*>(requestView) + RING_HEADER_BYTES,
    requestBytes - RING_HEADER_BYTES,
    requestEvent
  };

  auto responseSegment = new RingSegment{
    responseView,
    responseHeader,
    static_cast<uint8_t*>(responseView) + RING_HEADER_BYTES,
    responseBytes - RING_HEADER_BYTES,
    responseEvent
  };

  auto ring = new SharedRing(requestSegment, responseSegment);

  Napi::Object descriptor = Napi::Object::New(env);
  descriptor.Set("name", name);
  descriptor.Set("requestBytes", requestBytes);
  descriptor.Set("responseBytes", responseBytes);
  descriptor.Set("overflowBytes", overflowBytes);
  descriptor.Set("maxPayloadBytes", maxPayloadBytes);
  descriptor.Set("handle", Napi::External<SharedRing>::New(env, ring));
  return descriptor;
}

Napi::Value AttachSegment(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  if (info.Length() < 1 || !info[0].IsObject()) {
    Napi::TypeError::New(env, "attach(descriptor)").ThrowAsJavaScriptException();
    return env.Undefined();
  }

  Napi::Object descriptor = info[0].As<Napi::Object>();
  const std::string name = descriptor.Get("name").As<Napi::String>().Utf8Value();
  const uint32_t requestBytes = descriptor.Get("requestBytes").As<Napi::Number>().Uint32Value();
  const uint32_t responseBytes = descriptor.Get("responseBytes").As<Napi::Number>().Uint32Value();

  const std::string requestName = name + "_REQUEST";
  const std::string responseName = name + "_RESPONSE";
  const std::string requestEventName = name + "_REQUEST_EVENT";
  const std::string responseEventName = name + "_RESPONSE_EVENT";

  HANDLE requestMapping = OpenFileMappingA(FILE_MAP_ALL_ACCESS, FALSE, requestName.c_str());
  HANDLE responseMapping = OpenFileMappingA(FILE_MAP_ALL_ACCESS, FALSE, responseName.c_str());

  if (!requestMapping || !responseMapping) {
    Napi::Error::New(env, "OpenFileMapping failed").ThrowAsJavaScriptException();
    return env.Undefined();
  }

  void* requestView = MapViewOfFile(requestMapping, FILE_MAP_ALL_ACCESS, 0, 0, requestBytes);
  void* responseView = MapViewOfFile(responseMapping, FILE_MAP_ALL_ACCESS, 0, 0, responseBytes);

  if (!requestView || !responseView) {
    Napi::Error::New(env, "MapViewOfFile failed").ThrowAsJavaScriptException();
    return env.Undefined();
  }

  HANDLE requestEvent = OpenEventA(SYNCHRONIZE | EVENT_MODIFY_STATE, FALSE, requestEventName.c_str());
  HANDLE responseEvent = OpenEventA(SYNCHRONIZE | EVENT_MODIFY_STATE, FALSE, responseEventName.c_str());

  if (!requestEvent || !responseEvent) {
    Napi::Error::New(env, "OpenEvent failed").ThrowAsJavaScriptException();
    return env.Undefined();
  }

  auto requestHeader = static_cast<RingHeader*>(requestView);
  auto responseHeader = static_cast<RingHeader*>(responseView);

  auto requestSegment = new RingSegment{
    requestView,
    requestHeader,
    static_cast<uint8_t*>(requestView) + RING_HEADER_BYTES,
    requestBytes - RING_HEADER_BYTES,
    requestEvent
  };

  auto responseSegment = new RingSegment{
    responseView,
    responseHeader,
    static_cast<uint8_t*>(responseView) + RING_HEADER_BYTES,
    responseBytes - RING_HEADER_BYTES,
    responseEvent
  };

  auto ring = new SharedRing(requestSegment, responseSegment);

  return Napi::External<SharedRing>::New(env, ring);
}

Napi::Value PublishRequest(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  auto ring = info[0].As<Napi::External<SharedRing>>().Data();
  auto data = info[1].As<Napi::Uint8Array>();

  bool ok = WriteFrame(ring->request, data.Data(), static_cast<uint32_t>(data.ByteLength()));
  return Napi::Boolean::New(env, ok);
}

Napi::Value PublishResponse(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  auto ring = info[0].As<Napi::External<SharedRing>>().Data();
  auto data = info[1].As<Napi::Uint8Array>();

  bool ok = WriteFrame(ring->response, data.Data(), static_cast<uint32_t>(data.ByteLength()));
  return Napi::Boolean::New(env, ok);
}

Napi::Value ReadRequest(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  auto ring = info[0].As<Napi::External<SharedRing>>().Data();

  uint8_t* data;
  uint32_t length;
  uint32_t nextRead;

  if (!ReadFrame(ring->request, &data, &length, &nextRead)) return env.Null();

  Napi::ArrayBuffer buffer = Napi::ArrayBuffer::New(env, data, length, [](Napi::Env, void*) {});
  Napi::Uint8Array payload = Napi::Uint8Array::New(env, length, buffer, 0);

  Napi::Object result = Napi::Object::New(env);
  result.Set("payload", payload);
  result.Set("nextRead", nextRead);
  return result;
}

Napi::Value ReadResponse(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  auto ring = info[0].As<Napi::External<SharedRing>>().Data();

  uint8_t* data;
  uint32_t length;
  uint32_t nextRead;

  if (!ReadFrame(ring->response, &data, &length, &nextRead)) return env.Null();

  Napi::ArrayBuffer buffer = Napi::ArrayBuffer::New(env, data, length, [](Napi::Env, void*) {});
  Napi::Uint8Array payload = Napi::Uint8Array::New(env, length, buffer, 0);

  Napi::Object result = Napi::Object::New(env);
  result.Set("payload", payload);
  result.Set("nextRead", nextRead);
  return result;
}

Napi::Value CommitRequestRead(const Napi::CallbackInfo& info) {
  auto ring = info[0].As<Napi::External<SharedRing>>().Data();
  ring->request->header->read.store(info[1].As<Napi::Number>().Uint32Value(), std::memory_order_release);
  return info.Env().Undefined();
}

Napi::Value CommitResponseRead(const Napi::CallbackInfo& info) {
  auto ring = info[0].As<Napi::External<SharedRing>>().Data();
  ring->response->header->read.store(info[1].As<Napi::Number>().Uint32Value(), std::memory_order_release);
  return info.Env().Undefined();
}

Napi::Value WaitForRequest(const Napi::CallbackInfo& info) {
  auto ring = info[0].As<Napi::External<SharedRing>>().Data();
  const uint32_t timeout = info[1].As<Napi::Number>().Uint32Value();
  return Napi::Boolean::New(info.Env(), WaitForSingleObject(ring->request->event, timeout) == WAIT_OBJECT_0);
}

Napi::Value WaitForResponse(const Napi::CallbackInfo& info) {
  auto ring = info[0].As<Napi::External<SharedRing>>().Data();
  const uint32_t timeout = info[1].As<Napi::Number>().Uint32Value();
  return Napi::Boolean::New(info.Env(), WaitForSingleObject(ring->response->event, timeout) == WAIT_OBJECT_0);
}

Napi::Object InitModule(Napi::Env env, Napi::Object exports) {
  exports.Set("create", Napi::Function::New(env, CreateSegment));
  exports.Set("attach", Napi::Function::New(env, AttachSegment));
  exports.Set("publishRequest", Napi::Function::New(env, PublishRequest));
  exports.Set("publishResponse", Napi::Function::New(env, PublishResponse));
  exports.Set("readRequest", Napi::Function::New(env, ReadRequest));
  exports.Set("readResponse", Napi::Function::New(env, ReadResponse));
  exports.Set("commitRequestRead", Napi::Function::New(env, CommitRequestRead));
  exports.Set("commitResponseRead", Napi::Function::New(env, CommitResponseRead));
  exports.Set("waitForRequest", Napi::Function::New(env, WaitForRequest));
  exports.Set("waitForResponse", Napi::Function::New(env, WaitForResponse));
  return exports;
}

NODE_API_MODULE(wal_shared_memory, InitModule)
