package com.datatorrent.store;

import com.datatorrent.common.util.Slice;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.file.tfile.TFile;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

public class Writer
{
  FileSystem fs;

  public Writer(FileSystem fs)
  {
    this.fs = fs;
  }

  public FileInfo writeData(String fileName, Map<Slice, Slice> memBuf) throws IOException
  {
    StreamWriter writer = getStreamWriter(fileName);
    Slice startKey = null;
    Slice endKey = null;

    int count = 0;
    for (Map.Entry<Slice, Slice> entry : memBuf.entrySet()) {
      if (startKey == null)
        startKey = entry.getKey();
      writer.append(entry.getKey(), entry.getValue());
      endKey = entry.getKey();
      count++;
    }
    writer.close();
    long size = writer.getSize();

    FileInfo finfo = new FileInfo(fileName, size, count, startKey, endKey);
    return finfo;
  }

  StreamWriter getStreamWriter(String fileName) throws IOException
  {
    return new TFileStreamWriter(fs, fileName);
  }

  public interface StreamWriter extends Closeable {
    void append(Slice key, Slice value) throws IOException;
    long getSize() throws IOException;
  }

  static class TFileStreamWriter implements StreamWriter {

    FSDataOutputStream fout;
    TFile.Writer writer;

    public TFileStreamWriter(FileSystem fs, String fileName) throws IOException
    {
      fout = fs.create(new Path(fileName));
      writer = new TFile.Writer(fout, 32 * 1024, "none", "memcmp", new Configuration());
    }

    @Override public void append(Slice key, Slice value) throws IOException
    {
      writer.append(key.toByteArray(), value.toByteArray());
    }

    @Override public void close() throws IOException
    {
      writer.close();
      if(fout != null)
        fout.close();
    }

    public long getSize() throws IOException
    {
      return fout.getPos();
    }
  }
}
