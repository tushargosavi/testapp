package com.datatorrent.store;

import com.datatorrent.common.util.Slice;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.file.tfile.TFile;

import java.io.File;
import java.io.IOException;
import java.util.*;

/* Return edits to be done to metadata after compaction is over */
public interface Compactor
{
  VersionEdit compact() throws IOException;
}

interface CompactionStrategy {
  List<FileInfo> level0Files();
}

abstract class BaseCompator implements Compactor {
  Environment env;
  int baseLevel;
  int nextLevel;
  public Comparator<? super Slice> cmp;

  protected BaseCompator(Environment env)
  {
    this.env = env;
  }

  public void merge(List<FileInfo> lst1) throws IOException
  {
    Map<Slice, Slice> data = new TreeMap<Slice, Slice>(env.getComparator());
    for(FileInfo file : lst1) {
      System.out.println("reading file " + file);
      Scanner s = createScanner(file);
      readAll(s, data);
      s.close();
    }

    //System.out.println("flushing combined data for level " + level);
    //flushData(getNextFile(level+1), data);
  }

  protected void readAll(Scanner scanner, Map<Slice, Slice> data) throws IOException
  {
    scanner.rewind();
    int count = 0;
    while(!scanner.atEnd()) {
      System.out.println("reading key " + count);
      //TFile.Reader.Scanner.Entry e1 = scanner.entry();
      //in.put(getKeySlice(e1), getValSlice(e1));
      count++;
      scanner.advance();
    }
    System.out.println("reading done , read " + count);
  }

  protected Scanner createScanner(FileInfo file) throws IOException
  {
    return new TFileScanner(env.getFileSystem(), file);
  }
}

class Level0Compactor extends BaseCompator
{
  Index meta;
  VersionEdit edits;
  List<FileInfo> level0Files;
  List<FileInfo> level1Files;
  KeyRange range;
  Writer writer;

  public Level0Compactor(Environment env) {
    super(env);
    this.meta = env.getIndex();
    this.cmp = env.getComparator();
    edits = new VersionEdit();
    range = new KeyRange(null, null);
    writer = new Writer(env.getFileSystem());
  }

  /* get first file */
  List<FileInfo> getLeve0FilesToCompact()
  {
    FileInfo finfo = meta.get(0).get(0);
    List<FileInfo> lst  = new ArrayList();
    range.merge(env.getComparator(), new KeyRange(finfo.startKey, finfo.lastKey));
    lst.add(finfo);
    return lst;
  }

  List<FileInfo> getLevel1FilesForCompact()
  {
    //level1Files = meta.get(1).getFilesInRange(comparator, range);
    //range = range.merge(comparator, getRange(level1Files));
    return level1Files;
  }

  private KeyRange getRange(List<FileInfo> level0Files)
  {
    KeyRange range = new KeyRange(null, null);
    for(FileInfo f : level0Files) {
    //  range = range.merge(comparator, new KeyRange(f.startKey, f.lastKey));
    }
    return range;
  }

  @Override public VersionEdit compact() throws IOException
  {
    level0Files = getLeve0FilesToCompact();
    range = getRange(level0Files);
    level1Files = getLevel1FilesForCompact();
    String fileName = getNextFile(nextLevel);
    Writer.StreamWriter sw = writer.getStreamWriter(fileName);
    FileInfo merged = mergeFiles(level0Files);
    Slice small = null, last = null;
    Scanner s1 = createScanner(merged);
    Scanner s2 = new CombinedScanner(env.getFileSystem(), level1Files);
    s1.advance();
    s2.advance();
    while (!s1.atEnd() || !s2.atEnd()) {
      MutableKeyValPair e1 = s1.get();
      MutableKeyValPair e2 = s2.get();
      if (cmp.compare(e1.key, e2.key) < 0) {
        if (small != null)
          small = e1.key;
        sw.append(e1.key, e1.value);
        s1.advance();
        last = e1.key;
        e1 = s1.get();
      }
    }
    sw.close();
    FileInfo fi = new FileInfo(fileName, sw.getSize(), 0, small, last);

    edits.removeFile(baseLevel, level0Files);
    edits.removeFile(nextLevel, level1Files);
    edits.addFile(nextLevel, fi);
    return edits;
  }

  private FileInfo mergeFiles(List<FileInfo> files) throws IOException
  {
    TreeMap<Slice, Slice> data = new TreeMap<Slice, Slice>(cmp);
    for(FileInfo fi : files) {
      Scanner s = createScanner(fi);
      readAll(s, data);
    }
    FileInfo info = writer.writeData("tmpFile", data);
    return info;
  }

  public String getNextFile(int level) {
    int id = meta.get(level).getNextFileId();
    return "level_" + level + "_file_" + Integer.toString(id) + ".data";
  }

}
