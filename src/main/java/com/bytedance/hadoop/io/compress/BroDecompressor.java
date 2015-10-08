package com.bytedance.hadoop.io.compress;

import org.apache.hadoop.io.compress.Decompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;

/** */
public class BroDecompressor implements Decompressor {

  private static final Logger LOG = LoggerFactory.getLogger(BroDecompressor.class);

  static final int BUFFER_SIZE = 2048 * 1024;

  ByteBuffer in;
  ByteBuffer out;
  long nativePtr;
  long bufferPtr;
  boolean finalBlock;

  public BroDecompressor() {
    this.in = ByteBuffer.allocateDirect(BUFFER_SIZE);
    this.out = ByteBuffer.allocateDirect(BUFFER_SIZE);
    reinit();
  }

  void releaseNative() {
    if (bufferPtr != 0) {
      BroNative.freeBuffer(bufferPtr);
      bufferPtr = 0;
    }
    if (nativePtr != 0) {
      BroNative.freeDecompressionState(nativePtr);
      nativePtr = 0;
    }

  }

  void reinit() {
    releaseNative();
    this.in.limit(0).position(0);
    this.out.limit(BUFFER_SIZE).position(0);
    this.nativePtr = BroNative.initializeDecompressionState(null, 0);
    this.bufferPtr = 0;
    this.finalBlock = false;
  }

  @Override
  public void setInput(byte[] b, int off, int len) {
    //System.out.println("before set pos " + in.position() + " limit " + in.limit() + " put " + len);
    in.compact().put(b, off, len).limit(in.position()).position(0);
  }

  @Override
  public boolean needsInput() {
    return in.remaining() == 0;
  }

  @Override
  public void setDictionary(byte[] b, int off, int len) {

  }

  @Override
  public boolean needsDictionary() {
    return false;
  }

  @Override
  public boolean finished() {
    //System.out.println("in remain: " + in.remaining() + " final " + finalBlock + " bufferPtr " +
    //                   bufferPtr + " buf remain " + ((bufferPtr != 0) ? BroNative
     //   .remainInBuffer(bufferPtr) : -1));
    return finalBlock && in.remaining() == 0 && (
        bufferPtr == 0 || BroNative.remainInBuffer(bufferPtr) == 0
    );
  }

  @Override
  public int decompress(byte[] b, int off, int len) throws IOException {
    assert out.capacity() > len;
    int remain = len;
    boolean needMoreInput = false;
    while (remain > 0 && !needMoreInput) {
      long bufferRemain = (bufferPtr != 0) ? BroNative.remainInBuffer(bufferPtr) : 0;
      if (bufferRemain > 0) {
        out.position(0);
        int read = (int) BroNative.readBuffer(bufferPtr, (DirectBuffer) out, 0, len);
        out.get(b, off, read);
        remain -= read;
        off += read;
        //System.out.println("give " + read);
      } else {
        if (!finalBlock && in.remaining() == 0) {
          break;
        }
        BroNative.DecodeProgress p = BroNative.decodeBuffer(nativePtr, this.finalBlock ? 1 : 0,
                                                            (DirectBuffer) in, in.position(),
                                                            in.remaining(),
                                                            bufferPtr);
        if (p.result == 0) {
          throw new IOException("Corrupted");
        } else if (p.result == 2) {
          needMoreInput = true;
        } else if (p.result == 3) {
          throw new IOException("Output buffer overflow");
        }
        //System.out
        //    .println("inRead: " + p.inRead + ", write: " + p.outWritten + ", finalBlock " + finalBlock + " result " + p.result);
        in.position(in.position() + (int) p.inRead);
        bufferPtr = p.bufferPtr;
        if (finalBlock && BroNative.remainInBuffer(bufferPtr) == 0) {
          break;
        }
      }
    }
    return len - remain;
  }

  @Override
  public int getRemaining() {
    return in.remaining();
  }

  @Override
  public void reset() {
  }

  @Override
  public void end() {
    releaseNative();
  }

  @Override
  protected void finalize() throws Throwable {
    releaseNative();
    super.finalize();
  }

  public void finish() {
    finalBlock = true;
  }

}
