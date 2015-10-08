package com.bytedance.hadoop.io.compress;

import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.CompressorStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

/** */
public class BroCompressorStream extends CompressorStream {

  private static final Logger LOG = LoggerFactory.getLogger(BroCompressorStream.class);

  public BroCompressorStream(OutputStream out,
                             Compressor compressor, int bufferSize) {
    super(out, compressor, bufferSize);
  }

  public BroCompressorStream(OutputStream out, Compressor compressor) {
    super(out, compressor);
  }

  protected BroCompressorStream(OutputStream out) {
    super(out);
  }

  @Override
  public void finish() throws IOException {
    if (!compressor.finished()) {
      compressor.finish();
      while (!compressor.finished()) {
        compress();
      }
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    compressor.end();
  }
}
