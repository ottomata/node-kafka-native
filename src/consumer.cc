#include "consumer.h"
#include <nan.h>
#include <v8.h>
#include <errno.h>
#include <iostream>

#include "persistent-string.h"

using namespace v8;

Consumer::Consumer(Local<Object> &options):
    Common(RD_KAFKA_CONSUMER, options)
{
}

Consumer::~Consumer()
{
    // rd_kafka_topic_destroy() and rd_kafka_destroy() called in ~Common
}

Nan::Persistent<Function> Consumer::constructor;

void
Consumer::Init() {
    Nan::HandleScope scope;

    // Prepare constructor template
    Local<FunctionTemplate> tpl = Nan::New<FunctionTemplate>(New);
    tpl->SetClassName(Nan::New("Consumer").ToLocalChecked());
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    Nan::SetPrototypeMethod(tpl, "subscribe", WRAPPED_METHOD_NAME(Subscribe));
    Nan::SetPrototypeMethod(tpl, "unsubscribe", WRAPPED_METHOD_NAME(Unsubscribe));
    Nan::SetPrototypeMethod(tpl, "poll", WRAPPED_METHOD_NAME(Poll));
    Nan::SetPrototypeMethod(tpl, "close", WRAPPED_METHOD_NAME(Poll));

    //  TODO: implement assign, unassign, metadata, etc.

    // Nan::SetPrototypeMethod(tpl, "get_metadata", WRAPPED_METHOD_NAME(GetMetadata));

    constructor.Reset(tpl->GetFunction());
}

Local<Object>
Consumer::NewInstance(Local<Value> arg) {
    Nan::EscapableHandleScope scope;

    const unsigned argc = 1;
    Local<Value> argv[argc] = { arg };
    Local<Function> cons = Nan::New<Function>(constructor);
    Local<Object> instance = Nan::NewInstance(cons, argc, argv).ToLocalChecked();

    return scope.Escape(instance);
}

NAN_METHOD(Consumer::New) {

    if (!info.IsConstructCall()) {
        return Nan::ThrowError("non-constructor invocation not supported");
    }

    Local<Object> options(Nan::New<Object>());

    if (info.Length() == 1 && info[0]->IsObject()) {
        options = info[0].As<Object>();
    }

    Consumer* obj = new Consumer(options);
    obj->Wrap(info.This());

    std::string error;
    if (obj->consumer_init(&error)) {
        Nan::ThrowError(error.c_str());
        return;
    }

    info.GetReturnValue().Set(info.This());
}

int
Consumer::consumer_init(std::string *error) {
    Nan::HandleScope scope;

    // TODO move init to common?
    rd_kafka_conf_t *conf = rd_kafka_conf_new();

    // rd_kafka_conf_set_opaque(conf, this);

    int err = common_init(conf, error);
    if (err) {
        return err;
    }

    return 0;
}

WRAPPED_METHOD(Consumer, Subscribe) {
    Nan::HandleScope scope;

    if (info.Length() != 1 ||
        !( info[0]->IsObject()) ) {
        Nan::ThrowError("you must specify topics");
        return;
    }

    Local<Array> topic_list = info[0].As<Array>();
    rd_kafka_topic_partition_list_t *rd_kafka_topics;
    rd_kafka_topics = rd_kafka_topic_partition_list_new(topic_list->Length());
    for (size_t i = 0; i < topic_list->Length(); i++) {
        Local<Value> topic = Nan::Get(topic_list, i).ToLocalChecked().As<String>();
        String::Utf8Value utf8_topic(topic);
        // println("::::subscribing to %s, %s, %s", topic, utf8_topic, utf8_topic.c_str());
        rd_kafka_topic_partition_list_add(rd_kafka_topics, *utf8_topic, RD_KAFKA_PARTITION_UA);
    }

    rd_kafka_resp_err_t err = rd_kafka_subscribe(kafka_client_, rd_kafka_topics);
    rd_kafka_topic_partition_list_destroy(rd_kafka_topics);

    if (err) {
        std::string error_msg("Failed to set subscription: ");
        error_msg.append(rd_kafka_err2str(err));
        Nan::ThrowError(error_msg.c_str());
        return;
    }

    // TODO: Update rebalance callbacks

    return;
}

WRAPPED_METHOD(Consumer, Unsubscribe) {
    Nan::HandleScope scope;

    rd_kafka_resp_err_t err = rd_kafka_unsubscribe(kafka_client_);

    if (err) {
        std::string error_msg("Failed to remove subscription: ");
        error_msg.append(rd_kafka_err2str(err));
        Nan::ThrowError(error_msg.c_str());
        return;
    }

    return;
}

/**
 * Calls rd_kafka_consumer_poll and converts the returned
 * rd_kafka_message_t into an object.
 */
v8::Local<v8::Object>
Consumer::poll(uint32_t timeout_ms) {
    rd_kafka_message_t *msg;
    Local<Object> obj = Nan::New<Object>();

    static PersistentString topic_key("topic");
    static PersistentString partition_key("partition");
    static PersistentString offset_key("offset");
    static PersistentString value_key("value");
    static PersistentString key_key("key");
    static PersistentString errcode_key("errcode");
    static PersistentString errname_key("errname");
    static PersistentString errstr_key("errstr");

    // TODO: move msg -> object conversion into separate reusable function.
    msg = rd_kafka_consumer_poll(kafka_client_, timeout_ms);
    if (msg) {
        Nan::Set(obj, topic_key.handle(), Nan::New<String>(rd_kafka_topic_name(msg->rkt)).ToLocalChecked());
        Nan::Set(obj, partition_key.handle(), Nan::New<Number>(msg->partition));
        Nan::Set(obj, offset_key.handle(), Nan::New<Number>(msg->offset));

        // TODO: should we do an error object with functions like
        //       confluent-kafka-python?
        if (msg->err) {
            Nan::Set(obj, errcode_key.handle(), Nan::New<Number>(msg->err));
            Nan::Set(obj, errname_key.handle(), Nan::New<String>(rd_kafka_err2name(msg->err)).ToLocalChecked());
            Nan::Set(obj, errstr_key.handle(), Nan::New<String>(rd_kafka_err2str(msg->err)).ToLocalChecked());
        }

        if (msg->key_len) {
            Nan::Set(obj, key_key.handle(), Nan::New<String>((char*)msg->key, msg->key_len).ToLocalChecked());
        }
        if (msg->len) {
            Nan::Set(obj, value_key.handle(), Nan::New<String>((char*)msg->payload, msg->len).ToLocalChecked());
        }

        rd_kafka_message_destroy(msg);
    }

    return obj;
}

WRAPPED_METHOD(Consumer, Poll) {
    Nan::HandleScope scope;

    // Default timeout_ms to 1 second. TODO: Document
    uint32_t timeout_ms = 1000;

    // get first arg as Number and then convert to int
    if (info.Length() == 1 && info[0]->IsNumber()) {
        timeout_ms = Nan::To<uint32_t>(info[0].As<Number>()).FromJust();
    }

    // poll for a message and return it
    info.GetReturnValue().Set(this->poll(timeout_ms));
    return;
}


WRAPPED_METHOD(Consumer, Close) {
    Nan::HandleScope scope;

    rd_kafka_consumer_close(kafka_client_);
    return;
}

// WRAPPED_METHOD(Consumer, GetMetadata) {
//     Nan::HandleScope scope;
//     get_metadata(info);
//     return;
// }
