package com.datatorrent.store;

import com.datatorrent.common.util.Slice;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.file.tfile.TFile;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface Scanner  extends Closeable {
  void rewind() throws IOException;
  void advance() throws IOException;
  boolean atEnd();
  MutableKeyValPair get() throws IOException;
}

class TFileScanner implements Scanner
{
  private final TFile.Reader.Scanner scanner;
  private final TFile.Reader reader;
  private final FSDataInputStream di;

  public TFileScanner(FileSystem fs, FileInfo info) throws IOException
  {
    di = fs.open(new Path(info.path));
    reader = new TFile.Reader(di, info.size, new Configuration());
    scanner = reader.createScanner();
  }

  @Override public void advance() throws IOException
  {
    scanner.advance();
  }

  @Override public boolean atEnd()
  {
    return scanner.atEnd();
  }

  @Override public MutableKeyValPair get() throws IOException
  {
    TFile.Reader.Scanner.Entry e = scanner.entry();
    return new MutableKeyValPair(getKeySlice(e), getValSlice(e));
  }

  @Override public void rewind() throws IOException
  {
    scanner.rewind();
  }

  @Override public void close() throws IOException
  {
    scanner.close();
    reader.close();
    di.close();
  }

  // TODO share buffer.
  private Slice getValSlice(TFile.Reader.Scanner.Entry e) throws IOException
  {
    int valLen = e.getValueLength();
    byte[] val = new byte[valLen];
    e.getValue(val);
    return new Slice(val);
  }


  // TODO share buffer
  private Slice getKeySlice(TFile.Reader.Scanner.Entry e) throws IOException
  {
    int keyLen = e.getKeyLength();
    byte[] key = new byte[keyLen];
    e.getKey(key);
    return new Slice(key);
  }
}

class CombinedScanner implements Scanner {

  private TFileScanner scanner;
  FileSystem fs;
  List<FileInfo> files;
  int currFile;

  public CombinedScanner(FileSystem fs, List<FileInfo> files) throws IOException
  {
    this.fs = fs;
    this.files = files;
    if (files.size() > 0) {
      scanner = new TFileScanner(fs, files.get(0));
      currFile = 0;
    }
  }

  @Override public void advance() throws IOException
  {
    if (scanner == null) {
      if (currFile > files.size()) {
        throw new RuntimeException("Scanner called");
      }
    }
  }

  @Override public boolean atEnd()
  {
    return false;
  }

  @Override public MutableKeyValPair get() throws IOException
  {
    return null;
  }

  @Override public void rewind() throws IOException
  {

  }

  @Override public void close() throws IOException
  {

  }
}