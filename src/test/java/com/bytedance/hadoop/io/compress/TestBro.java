package com.bytedance.hadoop.io.compress;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.CompressorStream;
import org.apache.hadoop.io.compress.DecompressorStream;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** */
public class TestBro {

  void testOne(long seed, int q, int chunkSize, int chunkNumber, int entropy,
               int asciiOffset, int bufferSize)
      throws IOException {
    if (seed == -1)  {
      seed = System.currentTimeMillis();
    }
    System.out.println("BEGIN : Seed " + seed + " Quality " + q + " ChunkSize " + chunkSize
                       + " ChunkNumber " + chunkNumber + " Entropy " + entropy
                       + " AsciiOffset " + asciiOffset + " BufferSize " + bufferSize);
    FileOutputStream fosraw = new FileOutputStream("tmp_bro");
    FileOutputStream fos = new FileOutputStream("tmp_bro.bro");
    Compressor comp = new BroCompressor(q);
    comp.reinit(new Configuration());
    CompressorStream os = new BroCompressorStream(fos, comp);


    Random rnd = new Random(seed);
    for (int i = 0; i < chunkNumber; i++) {
      byte[] b = new byte[chunkSize];
      rnd.nextBytes(b);
      for (int i1 = 0; i1 < b.length; i1++) {
        b[i1] = (byte) (Math.abs(b[i1]) % entropy + asciiOffset);
      }
      os.write(b);
      fosraw.write(b);
    }
    os.close();
    fosraw.close();

    FileInputStream fin = new FileInputStream("tmp_bro.bro");
    DecompressorStream is = new BroDecompressorStream(fin, new BroDecompressor(), bufferSize);
    rnd = new Random(seed);
    for (int i = 0; i < chunkNumber; i++) {
      byte[] b = new byte[chunkSize];
      byte[] c = new byte[chunkSize];
      rnd.nextBytes(b);
      for (int i1 = 0; i1 < b.length; i1++) {
        b[i1] = (byte) (Math.abs(b[i1]) % entropy + asciiOffset);
      }
      int read = is.read(c);
      assertEquals(chunkSize, read);
      assertArrayEquals("Chunk number: " + i, b, c);
    }
    is.close();
    fin.close();
    System.out.println("PASSED: Quality " + q + " ChunkSize " + chunkSize
                       + " ChunkNumber " + chunkNumber + " Entropy " + entropy
                       + " AsciiOffset " + asciiOffset + " BufferSize " + bufferSize);
  }

  @Test
  public void testRandomData() throws IOException {
    int[] chunkSizes = new int[]{3333, 4096, 8192};
    int[] ents = new int[]{1, 10, 32, 208};
    int[] bufs = new int[]{333, 2048 * 1024};
    int[] nums = new int[]{0, 1, 2, 3, 5, 8, 10, 20, 30, 100, 10000};
    for (int num : nums) {
      for (int chunkSize : chunkSizes) {
        for (int ent : ents) {
          for (int buf : bufs) {
            testOne(-1, 1, chunkSize, num, ent + 48, 0, buf);
            testOne(-1, 5, chunkSize, num, ent + 48, 0, buf);
            testOne(-1, 11, chunkSize, num, ent, 48, buf);
          }
          System.gc();
        }
      }
    }
  }
}
