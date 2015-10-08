package com.bytedance.hadoop.io.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

/** */
public class TestBroCodec {

  private static final Logger LOG = LoggerFactory.getLogger(TestBroCodec.class);

  Configuration conf;
  FileSystem fs;
  BroCodec codec;

  @Before
  public void init() throws URISyntaxException, IOException {
    System.setProperty("user.dir", "/tmp");
    conf = new Configuration();
    fs = new RawLocalFileSystem();
    fs.initialize(new URI("file:///"), conf);
    codec = new BroCodec();
    codec.setConf(conf);
  }

  @Test
  public void testDecomp() throws IOException{
    Path p = new Path("test.bro");
    OutputStream out = codec.createOutputStream(fs.create(p));
    for (int i = 0; i < 100000; i ++) {
      out.write("gfi23weniogajn2o3ir4e2o3mta23krt23;'lkg'3a;r".getBytes());
    }
    out.close();

    InputStream in = codec.createInputStream(fs.open(p));
    for (int i = 0; i < 100000; i ++) {
      byte[] buf = new byte[256];
      in.read(buf);
    }
  }

  @After
  public void fina() throws IOException {
    fs.close();
  }
}
