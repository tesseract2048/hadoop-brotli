#include <fcntl.h>
#include <stdio.h>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <execinfo.h>
#include <signal.h>

#include "../dec/decode.h"
#include "../enc/encode.h"
#include "../enc/streams.h"

#include "com_bytedance_hadoop_io_compress_BroNative.h"
#include "com_bytedance_hadoop_io_compress_BroNative_OutputStorage.h"
#include "com_bytedance_hadoop_io_compress_BroNative_DecodeProgress.h"

#define COMPRESSOR ((brotli::BrotliCompressor *)(uint64_t)ptr)
#define STATE ((BrotliState*)(uint64_t)ptr)
#define BUFFER(x) ((OutBuffer*)(uint64_t)(x))

jclass c_OutputStorage;
jclass c_DecodeProgress;
jmethodID ctor_OutputStorage;
jmethodID ctor_DecodeProgress;
jfieldID f_OutputStorage_ptr;
jfieldID f_OutputStorage_size;
jfieldID f_OutputStorage_off;

void initJniStubs(JNIEnv *env) {
    // make global reference
    // FIXME: not releasing ref
    // FIXME: unexpected behavior with different class loader
    jclass localc_OutputStorage = env->FindClass("com/bytedance/hadoop/io/compress/BroNative$OutputStorage");
    c_OutputStorage = (jclass)env->NewGlobalRef(localc_OutputStorage);
    ctor_OutputStorage = env->GetMethodID(c_OutputStorage, "<init>", "(JJ)V");
    f_OutputStorage_ptr = env->GetFieldID(c_OutputStorage, "ptr", "J");
    f_OutputStorage_size = env->GetFieldID(c_OutputStorage, "size", "J");
    f_OutputStorage_off = env->GetFieldID(c_OutputStorage, "off", "J");

    jclass localc_DecodeProgress = env->FindClass("com/bytedance/hadoop/io/compress/BroNative$DecodeProgress");
    c_DecodeProgress = (jclass)env->NewGlobalRef(localc_DecodeProgress);
    ctor_DecodeProgress = env->GetMethodID(c_DecodeProgress, "<init>", "(JJJJ)V");
}

JNIEXPORT jlong JNICALL Java_com_bytedance_hadoop_io_compress_BroNative_initializeCompressor
  (JNIEnv *env, jclass klazz, jobject dict, jlong dictSize,
  jint quality, jint lgwin, jint lgblock, jint mode) {
    initJniStubs(env);

    brotli::BrotliParams params;
    params.quality = quality;
    params.mode = (brotli::BrotliParams::Mode) mode;
    params.lgwin = lgwin;
    params.lgblock = lgblock;
    brotli::BrotliCompressor *compressor = new brotli::BrotliCompressor(params);
    return (uint64_t)compressor;
}

JNIEXPORT void JNICALL Java_com_bytedance_hadoop_io_compress_BroNative_freeCompressor
    (JNIEnv *env, jclass klazz, jlong ptr) {
    delete COMPRESSOR;
}

JNIEXPORT jlong JNICALL Java_com_bytedance_hadoop_io_compress_BroNative_getBlockSize
  (JNIEnv *env, jclass klazz, jlong ptr) {
    return COMPRESSOR->input_block_size();
}

JNIEXPORT jlong JNICALL Java_com_bytedance_hadoop_io_compress_BroNative_copyOneBlockToRingBuffer
  (JNIEnv *env, jclass klazz, jlong ptr, jobject buf, jint off, jint len) {
    char *in = (char*)env->GetDirectBufferAddress(buf);
    COMPRESSOR->CopyInputToRingBuffer(len, (uint8_t*)&in[off]);
    return 0;
}

JNIEXPORT jobject JNICALL Java_com_bytedance_hadoop_io_compress_BroNative_compress
  (JNIEnv *env, jclass klazz, jlong ptr, jboolean finalBlock) {
    size_t out_bytes = 0;
    uint8_t* output;
    if (!COMPRESSOR->WriteBrotliData(finalBlock,
                                    false, &out_bytes, &output)) {
      return NULL;
    }
    return env->NewObject(c_OutputStorage, ctor_OutputStorage,
                          (jlong)output, (jlong)out_bytes);
}

JNIEXPORT jlong JNICALL Java_com_bytedance_hadoop_io_compress_BroNative_fillOutputBuffer
  (JNIEnv *env, jclass klazz, jlong ptr, jobject storage, jobject buf, jint off, jint len) {
    char* out = (char*)env->GetLongField(storage, f_OutputStorage_ptr);
    char* jbuf = (char*)env->GetDirectBufferAddress(buf);
    int out_offset = (int)env->GetLongField(storage, f_OutputStorage_off);
    int out_size = (int)env->GetLongField(storage, f_OutputStorage_size);
    int remain = out_size - out_offset;
    int read = (len > remain) ? remain : len;
    memcpy(&jbuf[off], &out[out_offset], read);
    env->SetLongField(storage, f_OutputStorage_off, out_offset + read);
    return read;
}

void handler(int sig) {
  void *array[32];
  size_t size;

  // get void*'s for all entries on the stack
  size = backtrace(array, 32);

  // print out all the frames to stderr
  fprintf(stderr, "Error: signal %d:\n", sig);
  backtrace_symbols_fd(array, size, STDERR_FILENO);
  exit(1);
}

JNIEXPORT jlong JNICALL Java_com_bytedance_hadoop_io_compress_BroNative_initializeDecompressionState
  (JNIEnv *env, jclass klazz, jobject dict, jlong dictSize) {
    //signal(SIGSEGV, handler);
    initJniStubs(env);
    BrotliState* state = new BrotliState;
    BrotliStateInit(state);
    return (jlong)state;
}

#define OUT_BUFFER_PAGE 65536

struct OutBufferPage {
    size_t used;
    size_t allocated;
    OutBufferPage* next;
    char* data;
};

struct OutBuffer {
    size_t offset;
    size_t used;
    size_t allocated;
    OutBufferPage* first;
    OutBufferPage* last;
};


OutBufferPage* allocatePage() {
    OutBufferPage* page = new OutBufferPage;
    page->used = 0;
    page->allocated = OUT_BUFFER_PAGE;
    page->data = (char*)malloc(OUT_BUFFER_PAGE);
    if (!page->data) {
        printf("Failed to allocate memory, decompression failed\n");
    }
    page->next = NULL;
    return page;
}

void addPage(OutBuffer* buffer) {
    OutBufferPage* newPage = allocatePage();
    if (buffer->first == NULL) {
        buffer->first = newPage;
    }
    if (buffer->last != NULL) {
        buffer->last->next = newPage;
    }
    buffer->last = newPage;
    buffer->allocated += newPage->allocated;
}

void freePage(OutBufferPage* page) {
    free(page->data);
    delete page;
}

void popPage(OutBuffer* buffer) {
    OutBufferPage* p = buffer->first;
    buffer->first = p->next;
    if (buffer->last == p) {
        buffer->last = NULL;
    }
    buffer->used -= p->used;
    buffer->allocated -= p->allocated;
    buffer->offset = 0;
    freePage(p);
}

OutBuffer* createBuffer() {
    OutBuffer* buffer = new OutBuffer;
    buffer->offset = 0;
    buffer->used = 0;
    buffer->allocated = 0;
    buffer->first = NULL;
    buffer->last = NULL;
    return buffer;
}

void freeBuffer(OutBuffer* buffer) {
    while (buffer->first != NULL) {
        popPage(buffer);
    }
    delete buffer;
}

int BrotliBufferOutputFunction(void* data, const uint8_t* buf, size_t count) {
  OutBuffer* buffer = (OutBuffer*)data;
  size_t remain = count;
  while (remain > 0) {
    OutBufferPage* page = buffer->last;
    if (page == NULL || page->used >= page->allocated) {
        addPage(buffer);
        continue;
    }
    size_t len = page->allocated - page->used;
    len = (len > remain) ? remain : len;
    memcpy(&page->data[page->used], &buf[count-remain], len);
    page->used += len;
    buffer->used += len;
    //printf("fill page %lx with %ld offset %ld, allocated: %ld used: %ld\n", (unsigned long)page, len, page->used-len, buffer->allocated, buffer->used);
    remain -= len;
  }
  return (int)count;
}

BrotliOutput bufferOutput(OutBuffer* buffer) {
  BrotliOutput out;
  out.cb_ = &BrotliBufferOutputFunction;
  out.data_ = buffer;
  return out;
}

JNIEXPORT jobject JNICALL Java_com_bytedance_hadoop_io_compress_BroNative_decodeBuffer
  (JNIEnv *env, jclass klazz, jlong ptr, jint finish, jobject in, jint inoff, jint inlen, jlong lastBufferPtr) {
    size_t available_in = inlen;
    const uint8_t* next_in = &((uint8_t*)env->GetDirectBufferAddress(in))[inoff];
    BrotliMemInput memin_;
    BrotliInput memin = BrotliInitMemInput(next_in, inlen, &memin_);
    OutBuffer* buffer = BUFFER(lastBufferPtr);
    if (buffer == NULL) {
        buffer = createBuffer();
    }
    size_t before = buffer->used;
    BrotliOutput bufout = bufferOutput(buffer);
    BrotliResult result = BrotliDecompressStreaming(memin, bufout, finish, STATE);
    size_t after = buffer->used;
    //printf("inlen: %d, input: %ld, before: %ld, after: %ld, result: %d, finish: %d\n", inlen, memin_.pos, before, after, result, finish);
    return env->NewObject(c_DecodeProgress, ctor_DecodeProgress,
                          (jlong)memin_.pos,
                          (jlong)after - before,
                          (jlong)buffer,
                          (jlong)result);
}

JNIEXPORT jlong JNICALL Java_com_bytedance_hadoop_io_compress_BroNative_remainInBuffer
  (JNIEnv *env, jclass klazz, jlong bufferPtr) {
    //printf("remainInBuffer %ld\n",  BUFFER(bufferPtr)->used - BUFFER(bufferPtr)->offset);
    return BUFFER(bufferPtr)->used - BUFFER(bufferPtr)->offset;
}

JNIEXPORT jlong JNICALL Java_com_bytedance_hadoop_io_compress_BroNative_readBuffer
  (JNIEnv *env, jclass klazz, jlong bufferPtr, jobject out, jint outoff, jint outlen) {
    //printf("Java_com_bytedance_hadoop_io_compress_BroNative_readBuffer\n");
    char* outarr = &((char*)env->GetDirectBufferAddress(out))[outoff];
    size_t remain = outlen;
    OutBuffer* buffer = BUFFER(bufferPtr);
    //printf("used: %ld, allocated: %ld\n",  buffer->used, buffer->allocated);
    while (remain > 0 && (buffer->used - buffer->offset) > 0) {
        if (!buffer->first) {
            //printf("buffer used %ld > 0 but no first page (allocated: %ld)\n", buffer->used, buffer->allocated);
            break;
        }
        int pageRemain = buffer->first->used - buffer->offset;
        int readLen = (pageRemain > remain) ? remain : pageRemain;
        //printf("copy page %lx offset %ld len %d\n", (unsigned long)buffer->first, buffer->offset, readLen);
        memcpy(&outarr[outlen-remain], &buffer->first->data[buffer->offset], readLen);
        buffer->offset += readLen;
        if (buffer->offset >= buffer->first->allocated) {
            //printf("poppage\n");
            popPage(buffer);
        }
        remain -= readLen;
        //printf("read %d, remain: %ld, offset: %ld, used: %ld, allocated: %ld\n", readLen, remain, buffer->offset, buffer->used, buffer->allocated);
    }
    return outlen - remain;
}

JNIEXPORT void JNICALL Java_com_bytedance_hadoop_io_compress_BroNative_freeBuffer
  (JNIEnv *env, jclass klazz, jlong bufferPtr) {
    freeBuffer(BUFFER(bufferPtr));
}

JNIEXPORT void JNICALL Java_com_bytedance_hadoop_io_compress_BroNative_freeDecompressionState
  (JNIEnv *env, jclass klazz, jlong ptr) {
    BrotliStateCleanup(STATE);
    delete STATE;
}
