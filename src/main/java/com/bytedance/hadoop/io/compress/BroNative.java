package com.bytedance.hadoop.io.compress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.nio.ch.DirectBuffer;

/** */
public class BroNative {

  private static final Logger LOG = LoggerFactory.getLogger(BroNative.class);

  static {
    try {
      System.loadLibrary("brojni");
      LOG.debug("Loaded brotli native library");
    } catch (Throwable t) {
      LOG.error("brotli native library not loaded");
      throw t;
    }
  }

  static class OutputStorage {

    final long ptr;
    final long size;
    long off;

    public OutputStorage(long ptr, long size) {
      this.ptr = ptr;
      this.size = size;
      this.off = 0;
    }
  }

  static class DecodeProgress {

    final long inRead;
    final long outWritten;
    final long bufferPtr;
    final long result;

    public DecodeProgress(long inRead, long outWritten, long bufferPtr, long result) {
      this.inRead = inRead;
      this.outWritten = outWritten;
      this.bufferPtr = bufferPtr;
      this.result = result;
    }
  }

  static native long initializeCompressor(DirectBuffer dict, long dictSize,
                                          int quality, int lgwin, int lgblock,
                                          int mode);

  static native void freeCompressor(long ptr);

  static native long getBlockSize(long ptr);

  static native long copyOneBlockToRingBuffer(long ptr, DirectBuffer data, int off, int len);

  static native OutputStorage compress(long ptr, boolean finalBlock);

  static native long fillOutputBuffer(long ptr, OutputStorage storage, DirectBuffer buf, int off,
                                      int len);

  static native long initializeDecompressionState(DirectBuffer dict, long dictSize);

  static native DecodeProgress decodeBuffer(long ptr, int finish,
                                            DirectBuffer in, int inoff, int inlen,
                                            long lastBufferPtr);

  static native long remainInBuffer(long bufferPtr);

  static native long readBuffer(long bufferPtr, DirectBuffer out, int outoff, int outlen);

  static native void freeBuffer(long bufferPtr);

  static native void freeDecompressionState(long ptr);

}
