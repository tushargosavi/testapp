package com.datatorrent.store;

import com.datatorrent.common.util.Slice;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.file.tfile.TFile;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class TestStore
{
  static final int maxKeyLength = 10;
  static final Random random = new Random();

  static Slice generateRandomSlice(int n)
  {
    byte[] data = new byte[n];
    random.nextBytes(data);
    return new Slice(data);
  }

  static Slice genKey() {
    return generateRandomSlice(maxKeyLength);
  }

  static Slice genVal() {
    return generateRandomSlice(random.nextInt(100));
  }

  @Test
  public void test() throws IOException, InterruptedException
  {
    DefaultKeyValStore store = new DefaultKeyValStore();
    store.setBasePath("test");
    store.setup();
    for(int file = 0; file < 10; file++) {
      for (int i = 0; i < 1000; i++) {
        store.put(genKey(), genVal());
      }
      System.out.println("comminting data");
      store.commit();
      System.out.println("commit done");
    }
    store.close();
  }

  @Test
  public void test1() throws IOException
  {
    String basePath = "test/testfile";
    Path path = new Path(basePath + "/file1");
    FileSystem fs = FileSystem.newInstance(new Path(basePath).toUri(), new Configuration());
    fs.setVerifyChecksum(false);
    FSDataOutputStream out = fs.create(path);
    TFile.Writer writer = new TFile.Writer(out, 32 * 1024, "none", "memcmp", new Configuration());
    TreeMap<Slice, Slice> memBuf = new TreeMap<Slice, Slice>(new SliceComparator());
    for (int i = 0; i < 10000; i++) {
      memBuf.put(genKey(), genVal());
    }
    for(Map.Entry<Slice, Slice> entry : memBuf.entrySet()) {
      writer.append(entry.getKey().toByteArray(), entry.getValue().toByteArray());
    }
    writer.close();
    out.close();

    FileStatus fstat = fs.getFileStatus(path);
    long size = fstat.getLen();
    long size1 = out.getPos();
    System.out.println("size of file is " + size + " size1 " + size1);
    FSDataInputStream fin = fs.open(new Path(basePath + "/file1"));
    TFile.Reader reader = new TFile.Reader(fin, size1, new Configuration());
    TFile.Reader.Scanner scanner = reader.createScanner();
  }
}
