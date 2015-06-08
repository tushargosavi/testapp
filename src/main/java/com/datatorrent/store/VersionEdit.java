package com.datatorrent.store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class VersionEdit
{
  private List<AddFile> newFiles = new ArrayList<AddFile>();
  private List<DelFile> delFiles = new ArrayList<DelFile>();

  public void addFile(int level, FileInfo finfo) {

    newFiles.add(new AddFile(level, finfo));
  }

  public void addFiles(int level, Collection<FileInfo> files) {
    for(FileInfo f : files) {
      addFile(level, f);
    }
  }

  public void removeFile(int level, FileInfo finfo) {
    delFiles.add(new DelFile(level, finfo));
  }

  public void removeFile(int level, Collection<FileInfo> files) {
    for(FileInfo f : files) {
      removeFile(level, f);
    }
  }

  public void merge(VersionEdit delta) {
    this.newFiles.addAll(delta.newFiles);
    this.delFiles.addAll(delta.delFiles);
  }

  public static class AddFile {
    final int level;
    final FileInfo info;

    public AddFile(int level, FileInfo finfo)
    {
      this.level = level;
      this.info = finfo;
    }
  };

  public static class DelFile {
    final int level;
    final FileInfo info;

    public DelFile(int level, FileInfo finfo)
    {
      this.level = level;
      this.info = finfo;
    }
  }
}
