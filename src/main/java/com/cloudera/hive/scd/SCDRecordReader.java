/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.hive.scd;

import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

/**
 * A {@link RecordReader} that can apply a series of SQL updates/deletes on the
 * records as they are read.
 */
public class SCDRecordReader<K, V> implements RecordReader<K, V> {

  private final RecordReader<K, V> delegate;
  private final SQLUpdater<K, V> updater;

  public SCDRecordReader(RecordReader<K, V> delegate, SQLUpdater<K, V> updater) {
    this.delegate = delegate;
    this.updater = updater;
  }

  @Override
  public boolean next(K k, V v) throws IOException {
    if (delegate.next(k, v)) {
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
