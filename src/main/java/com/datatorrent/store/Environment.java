package com.datatorrent.store;

import com.datatorrent.common.util.Slice;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;
import java.util.UUID;

public class Environment implements Closeable
{
  FileSystem fileSystem;
  String baseDir;
  Comparator comparator;
  Index index;

  public Environment(FileSystem fileSystem, String baseDir, Comparator cmp) {
    this.fileSystem = fileSystem;
    this.baseDir = baseDir;
    this.comparator = cmp;
  }

  public Environment(String baseDir) throws IOException
  {
    Path basePath = new Path(baseDir);
    fileSystem = FileSystem.newInstance(basePath.toUri(), new Configuration());
    fileSystem.setWriteChecksum(false);
    fileSystem.setVerifyChecksum(false);
    this.comparator = new SliceComparator();
    index = new Index();
  }

  public Comparator<? super Slice> getComparator()
  {
    return comparator;
  }

  public FileSystem getFileSystem()
  {
    return fileSystem;
  }

  @Override public void close() throws IOException
  {
    if (fileSystem != null)
      fileSystem.close();
  }

  public Index getIndex()
  {
    return index;
  }

  static class EnvironentBuilder {
    private Comparator cmp;
    private String baseDir;
    private Path basePath;

    public EnvironentBuilder setComparator(Comparator cmp) {
      this.cmp = cmp;
      return this;
    }
    public EnvironentBuilder setBasePath(String baseDir) {
      this.baseDir = baseDir;
      return this;
    }
    Environment build() throws IOException
    {
      basePath = new Path(baseDir);
      FileSystem fs = FileSystem.newInstance(basePath.toUri(), new Configuration());
      return new Environment(fs, baseDir, new SliceComparator());
    }
  }

  public Path getFilePath(int level, int id) {
      return new Path(baseDir + "/level_" + level + "_file_" + Integer.toString(id) + ".data");
  }

  public Path getTempFile() throws IOException
  {
    while (true) {
      String uuid = UUID.randomUUID().toString();
      Path p = new Path(baseDir + "/tmp/" + uuid);
      if (fileSystem.exists(p))
        continue;
      return p;
    }
  }

  public void setup() throws IOException
  {
    cleanupTemp();

    /* load index from disk */
    Path p = getIndexPath();
    if (fileSystem.exists(p)) {
      FSDataInputStream in = fileSystem.open(p);
      index.load(in);
    }
  }

  private void cleanupTemp() throws IOException
  {
    Path p = new Path(baseDir + "/tmp/");
    fileSystem.delete(p, true);
  }

  public void writeIndex() throws IOException
  {
    FSDataOutputStream os = fileSystem.create(getIndexPath());
    index.save(os);
    os.close();
  }

  private Path getIndexPath()
  {
    return new Path(baseDir + "/INDEX");
  }


  public Path getNextFile(int level) {
    int id = index.get(level).getNextFileId();
    return new Path(baseDir + "/level_" + level + "_file_" + Integer.toString(id) + ".data");
  }
}
