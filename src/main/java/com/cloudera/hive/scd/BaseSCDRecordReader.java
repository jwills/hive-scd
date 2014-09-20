package com.cloudera.hive.scd;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class BaseSCDRecordReader<K, V> extends RecordReader<K, V> {

  private final RecordReader<K, V> delegate;
  private final SQLUpdater<K, V> updater;

  public BaseSCDRecordReader(RecordReader<K, V> delegate, SQLUpdater<K, V> updater) {
    this.delegate = delegate;
    this.updater = updater;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext ctxt) throws IOException, InterruptedException {
    delegate.initialize(split, ctxt);
    updater.initialize(split, ctxt);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (delegate.nextKeyValue()) {
      boolean skip = updater.apply(delegate.getCurrentKey(), delegate.getCurrentValue());
      while (skip && delegate.nextKeyValue()) {
        skip = updater.apply(delegate.getCurrentKey(), delegate.getCurrentValue());
      }
      return !skip;
    }
    return false;
  }

  @Override
  public K getCurrentKey() throws IOException, InterruptedException {
    return delegate.getCurrentKey();
  }

  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return delegate.getCurrentValue();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return delegate.getProgress();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
    updater.close();
  }
}
