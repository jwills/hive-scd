package com.cloudera.hive.scd;

import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

public class BaseSCDRecordReader<K, V> implements RecordReader<K, V> {

  private final RecordReader<K, V> delegate;
  private final SQLUpdater<K, V> updater;

  public BaseSCDRecordReader(RecordReader<K, V> delegate, SQLUpdater<K, V> updater) {
    this.delegate = delegate;
    this.updater = updater;
  }

  @Override
  public boolean next(K k, V v) throws IOException {
    if (delegate.next(k, v)) {
      System.out.println("Applying update...");
      boolean skip = updater.apply(k, v);
      while (skip && delegate.next(k, v)) {
        skip = updater.apply(k, v);
      }
      return !skip;
    }
    return false;
  }

  @Override
  public K createKey() {
    return delegate.createKey();
  }

  @Override
  public V createValue() {
    return delegate.createValue();
  }

  @Override
  public long getPos() throws IOException {
    return delegate.getPos();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
    updater.close();
  }

  @Override
  public float getProgress() throws IOException {
    return delegate.getProgress();
  }
}
