package com.bytedance.hadoop.io.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;

/** */
public class BroCompressor implements Compressor {

  private static final Logger LOG = LoggerFactory.getLogger(BroCompressor.class);

  static final int BUFFER_SIZE = 2048 * 1024;

  long nativePtr;
  BroNative.OutputStorage currentOut;
  boolean finalBlock;
  long bytesRead;
  long bytesWritten;
  ByteBuffer buffer;

  byte[] currentIn;
  int currentInOff;
  int currentInLimit;

  int ringRemain;
  int blockSize;
  boolean initialized;
  boolean canFinish;

  final int quality;

  public BroCompressor(int quality) {
    nativePtr = 0;
    initialized = false;
    this.quality = quality;
  }

  void reinit() {
    if (nativePtr != 0) {
      BroNative.freeCompressor(nativePtr);
      nativePtr = 0;
    }
    nativePtr = BroNative.initializeCompressor(null, 0, quality, 22, 0, 0);
    currentOut = null;
    finalBlock = false;
    bytesRead = 0;
    bytesWritten = 0;
    buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    currentIn = null;
    blockSize = (int) BroNative.getBlockSize(nativePtr);
    ringRemain = blockSize;
    initialized = true;
    canFinish = false;
  }

  @Override
  public void setInput(byte[] b, int off, int len) {
    currentIn = b;
    currentInOff = off;
    currentInLimit = off + len;
  }

  @Override
  public boolean needsInput() {
    //System.out.println("currentInLimit " + currentInLimit + ", currentInOff " + currentInOff);
    if (currentIn == null || currentInLimit == currentInOff) {
      return true;
    }
    return false;
  }

  @Override
  public void setDictionary(byte[] b, int off, int len) {

  }

  @Override
  public long getBytesRead() {
    return 0;
  }

  @Override
  public long getBytesWritten() {
    return 0;
  }

  @Override
  public void finish() {
    finalBlock = true;
    if (bytesRead == 0) {
      canFinish = true;
    }
  }

  @Override
  public boolean finished() {
    return canFinish && (currentOut == null || currentOut.size == currentOut.off);
  }

  int consumeOutput(byte[] b, int off, int len) {
    int consumeLen = (int) Math.min(len, currentOut.size - currentOut.off);
    buffer.position(0);
    int ret = (int) BroNative.fillOutputBuffer(nativePtr, currentOut,
                                               (DirectBuffer) buffer, 0, consumeLen);
    buffer.get(b, off, ret);
    bytesWritten += ret;
    return ret;
  }

  @Override
  public int compress(byte[] b, int off, int len) throws IOException {
    if (currentOut == null || currentOut.size == 0 || currentOut.size == currentOut.off) {
      if (currentIn == null) {
        return 0;
      }
      buffer.position(0);
      int putLen = Math.min(ringRemain, currentInLimit - currentInOff);
      buffer.put(currentIn, currentInOff, putLen);
      currentInOff += putLen;
      //System.out.println("putLen: " + putLen);
      BroNative.copyOneBlockToRingBuffer(nativePtr, (DirectBuffer) buffer, 0, putLen);
      ringRemain -= putLen;
      bytesRead += putLen;
      if (ringRemain > 0 && !finalBlock) {
        return 0;
      }
      currentOut = BroNative.compress(nativePtr, finalBlock);
      //System.out.println("finalBlock: " + finalBlock + ", putLen: " + putLen +
      //                   ", ringRemain: " + ringRemain + ", out: " + );
      if (finalBlock) {
        canFinish = true;
      }
      if (currentOut == null) {
        throw new IOException("Failed to compress");
      }
      ringRemain = blockSize;
      if (currentOut.size == 0) {
        return 0;
      }
    }
    return consumeOutput(b, off, len);
  }

  @Override
  public void reset() {
    reinit();
  }

  @Override
  protected void finalize() throws Throwable {
    if (nativePtr != 0) {
      BroNative.freeCompressor(nativePtr);
      nativePtr = 0;
    }
    super.finalize();
  }

  @Override
  public void end() {
    if (nativePtr != 0) {
      BroNative.freeCompressor(nativePtr);
      nativePtr = 0;
    }
  }

  @Override
  public void reinit(Configuration conf) {
    reinit();
  }
}
