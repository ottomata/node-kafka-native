#pragma once

#include <node.h>
#include <nan.h>
#include <utility>
#include "common.h"
#include "wrapped-method.h"

class ConsumerLoop;

class Consumer : public Common {
public:
    static void Init();
    static v8::Local<v8::Object> NewInstance(v8::Local<v8::Value> arg);
    int consumer_init(std::string *error);

private:
    explicit Consumer(v8::Local<v8::Object> &options);
    ~Consumer();
    Consumer(const Consumer &) = delete;
    Consumer &operator=(const Consumer &) = delete;
    v8::Local<v8::Object> poll(uint32_t timeout_ms);

    static NAN_METHOD(New);

    WRAPPED_METHOD_DECL(Subscribe);
    WRAPPED_METHOD_DECL(Unsubscribe);
    WRAPPED_METHOD_DECL(Poll);
    WRAPPED_METHOD_DECL(Close);

    // WRAPPED_METHOD_DECL(GetMetadata);

    static Nan::Persistent<v8::Function> constructor;
};
