package com.bytedance.hadoop.io.compress;

import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DecompressorStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/** */
public class BroDecompressorStream extends DecompressorStream {

  private static final Logger LOG = LoggerFactory.getLogger(BroDecompressorStream.class);


  public BroDecompressorStream(InputStream in,
                               Decompressor decompressor,
                               int bufferSize) throws IOException {
    super(in, decompressor, bufferSize);
  }

  public BroDecompressorStream(InputStream in, Decompressor decompressor) throws IOException {
    super(in, decompressor);
  }

  protected BroDecompressorStream(InputStream in) throws IOException {
    super(in);
  }

  @Override
  public void close() throws IOException {
    super.close();
    decompressor.end();
  }

  @Override
  protected int decompress(byte[] b, int off, int len) throws IOException {

    int n = 0;

    while (len > 0) {
      int read =  decompressor.decompress(b, off, len);
      n += read;
      off += read;
      len -= read;
      if (len == 0) {
        break;
      }
      if (decompressor.finished()) {
        eof = true;
        return (n > 0) ? n : -1;
      }

      int m = getCompressedData();
      if (m == -1) {
        ((BroDecompressor) decompressor).finish();
        continue;
      }
      decompressor.setInput(buffer, 0, m);
    }

    return n;
  }
}
