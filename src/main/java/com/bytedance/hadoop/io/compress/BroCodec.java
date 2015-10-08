package com.bytedance.hadoop.io.compress;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** */
public class BroCodec implements Configurable, CompressionCodec {

  private static final Logger LOG = LoggerFactory.getLogger(BroCodec.class);

  int quality = 6;
  int bufferSize = 1024 * 2048;
  Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.quality = conf.getInt("bro.quality", 6);
    this.bufferSize = conf.getInt("bro.buffer-size", 1024 * 2048);
    LOG.info("brolti quality: " + this.quality + ", buffer size: " + this.bufferSize);
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public Compressor createCompressor() {
    BroCompressor comp = new BroCompressor(quality);
    comp.reinit(conf);
    return comp;
  }

  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    return BroDecompressor.class;
  }

  @Override
  public Decompressor createDecompressor() {
    return new BroDecompressor();
  }

  @Override
  public String getDefaultExtension() {
    return ".bro";
  }

  @Override
  public Class<? extends Compressor> getCompressorType() {
    return BroCompressor.class;
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in) throws IOException {
    return new BroDecompressorStream(in, createDecompressor(), bufferSize);
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor)
      throws IOException {
    return new BroDecompressorStream(in, decompressor, bufferSize);
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor)
      throws IOException {
    return new BroCompressorStream(out, compressor, bufferSize);
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
    return new BroCompressorStream(out, createCompressor(), bufferSize);
  }
}
