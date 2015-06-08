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
  private int maxSize = 32 * 1024 * 1024;
  private int memUsed;
  String basePath;
  private FileSystem fs;
  private Index index;
  private transient ExecutorService executorService;
  private Comparator<Slice> comparator = new SliceComparator();
  TreeMap<Slice, Slice> memBuf = new TreeMap<Slice, Slice>(comparator);
  private boolean level_o_compaction_active = false;
  private AtomicInteger[] fileIds = new AtomicInteger[9];

  DefaultKeyValStore() {

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

  public String getNextFile(int level) {
    int id = fileIds[level].getAndAdd(1);
    return basePath + "/level_" + level + "_file_" + Integer.toString(id) + ".data";
  }

  void setup() throws IOException
  {
    Path baseDir = new Path(basePath);
    fs = FileSystem.newInstance(baseDir.toUri(), new Configuration());
    fs.setWriteChecksum(false);
    fs.setVerifyChecksum(false);
    executorService = Executors.newScheduledThreadPool(10);
    for(int i = 0; i < 9; i++) {
      fileIds[i] = new AtomicInteger(0);
    }
    index = new Index();
  }

  void close() throws IOException, InterruptedException
  {
    executorService.awaitTermination(1, TimeUnit.DAYS);
    if (fs != null)
      fs.close();
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
    VersionEdit finfo = flushData(getNextFile(0));

  }

  private VersionEdit flushData(String fileName) throws IOException
  {
    return flushData(fileName, memBuf);
  }

  private VersionEdit flushData(String fileName, Map<Slice, Slice> memBuf) throws IOException
  {
    System.out.println("flusing data to file " + fileName);
    FSDataOutputStream fout = fs.create(new Path(fileName));
    TFile.Writer writer = new TFile.Writer(fout, 32 * 1024, "none", "memcmp", new Configuration());
    Slice startKey = null;
    Slice endKey = null;

    int count = 0;
    for (Map.Entry<Slice, Slice> entry : memBuf.entrySet()) {
      if (startKey == null) startKey = entry.getKey();
      writer.append(entry.getKey().toByteArray(), entry.getValue().toByteArray());
      endKey = entry.getKey();
      count++;
    }
    writer.close();
    long size = fout.getPos();
    fout.close();

    FileInfo finfo = new FileInfo(fileName, size, count, startKey, endKey);
    VersionEdit edit = new VersionEdit();
    edit.addFile(9, finfo);
    return edit;
  }

  public void level0_add(FileInfo finfo) {
    LevelIndex linfo = index.get(0);
    linfo.addFile(finfo);
    if (linfo.numFiles() > MAX_L0_FILES) {
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
    FileInfo file1 = index.get(0).get(0);
    Slice start = file1.startKey;
    Slice end = file1.lastKey;

    /* find matching files in level n */
    List<FileInfo> list = getFilesInRange(1, start, end);
    List<FileInfo> lst1 = new ArrayList<FileInfo>(list);
    lst1.add(file1);
    // TODO get list of files from level 0 which might be having key range in same
    // and merge them and them compact with level 1 files.

    System.out.println("files to be compacted ");
    for(FileInfo f : lst1) {
      System.out.println(f.path);
    }
    for(FileInfo f : list) {
      System.out.println(f.path);
    }

    //compactFiles(0, lst1, list);
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


  /* Returns list of file which conatains key ranges between start and end
   * from level lvl */
  List<FileInfo> getFilesInRange(int lvl, Slice start, Slice end) {
    List<FileInfo> includeFiles = new ArrayList<FileInfo>();
    LevelIndex lidx = index.get(lvl);
    for(FileInfo file : lidx.files) {
      if (comparator.compare(file.lastKey, start) >= 0 &&
          (comparator.compare(file.startKey, end) <= 0)) {
        includeFiles.add(file);
      }
    }
    return includeFiles;
  }

  TFile.Reader.Scanner createScanner(FileInfo finfo) throws IOException
  {
    FSDataInputStream di = fs.open(new Path(finfo.path));
    TFile.Reader reader = new TFile.Reader(di, finfo.size, new Configuration());
    TFile.Reader.Scanner scanner = reader.createScanner();
    return scanner;
  }
}
