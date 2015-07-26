package com.datatorrent.store;

import com.datatorrent.common.util.Slice;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.file.tfile.TFile;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultKeyValStore implements KeyValStore
{
  private static final int MAX_L0_FILES = 4;
  public String basePath;
  private int maxSize = 32 * 1024 * 1024;
  private transient ExecutorService executorService;
  TreeMap<Slice, Slice> memBuf;
  private boolean level_o_compaction_active = false;
  private Writer writer;

  Environment env;
  private int memUsed;

  DefaultKeyValStore() throws IOException
  {

  }

  public int getMaxSize()
  {
    return maxSize;
  }

  public void setMaxSize(int maxSize)
  {
    this.maxSize = maxSize;
  }

  public String getBasePath()
  {
    return basePath;
  }

  public void setBasePath(String basePath)
  {
    this.basePath = basePath;
  }

  public static int getMaxL0Files()
  {
    return MAX_L0_FILES;
  }


  void setup() throws IOException
  {
    executorService = Executors.newScheduledThreadPool(10);
    env = new Environment(basePath);
    memBuf = new TreeMap<Slice, Slice>(env.getComparator());
    writer = new Writer(env.getFileSystem());
  }

  void close() throws IOException, InterruptedException
  {
    executorService.awaitTermination(5, TimeUnit.SECONDS);
    env.close();
  }

  @Override public void put(Slice key, Slice value)
  {
    memBuf.put(key, value);
    memUsed = key.length + value.length;
    // TODO update log
  }

  @Override public Slice get(Slice key)
  {
    return null;
  }

  void commit() throws IOException
  {
    VersionEdit finfo = flushData(env.getNextFile(0));
    env.writeIndex();
  }

  Path getIndexPath() {
    return new Path(basePath + "/" + "INDEX");
  }

  private VersionEdit flushData(Path fileName) throws IOException
  {
    return flushData(fileName, memBuf);
  }

  private VersionEdit flushData(Path fileName, Map<Slice, Slice> memBuf) throws IOException
  {
    FileInfo newFile =  writer.writeData(fileName.toString(), memBuf);
    VersionEdit edit = new VersionEdit();
    edit.addFile(0, newFile);
    return edit;
  }

  public void level0_add(VersionEdit edit) {
    Index idx = env.getIndex();
    env.getIndex().apply(edit);
    if (idx.get(0).numFiles() > MAX_L0_FILES) {
      start_level_0_compaction();
    }
  }

  synchronized private void start_level_0_compaction()
  {
    /* avoid running multiple level 0 compaction together */
    if (level_o_compaction_active)
      return;
    level_o_compaction_active = true;
    Runnable task = new Runnable() {
      @Override public void run()
      {
        try {
          level_0_compaction();
        } catch (IOException e) {
          e.printStackTrace();
          level_o_compaction_active = false;
        }
      }
    };
    executorService.submit(task);
  }

  private void level_0_compaction() throws IOException
  {
    Level0Compactor compator = new Level0Compactor(env);
    compator.compact();
    level_o_compaction_active = false;
  }

  private int compareEntries(TFile.Reader.Scanner.Entry e1, TFile.Reader.Scanner.Entry e2) throws IOException
  {
    byte[] otherKey = getKey(e2);
    return e1.compareTo(otherKey);
  }

  private byte[] getVal(TFile.Reader.Scanner.Entry e) throws IOException
  {
    int valLen = e.getValueLength();
    byte[] val = new byte[valLen];
    e.getKey(val);
    return val;
  }

  private byte[] getKey(TFile.Reader.Scanner.Entry e) throws IOException
  {
    int keyLen = e.getKeyLength();
    byte[] key = new byte[keyLen];
    e.getKey(key);
    return key;
  }

}
