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

import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;

/**
 * Base class for performing DML updates on the content read in from a {@code }RecordReader} via the
 * {@link com.cloudera.hive.scd.SCDRecordReader}. The updates should be contained in a file named
 * ".updates" that is located in the same directory as the regular input files.
 */
public abstract class SQLUpdater<K, V> {

  private Connection conn;
  private DMLHelper<K, V> helper;
  private List<PreparedStatement> updateStmts;
  private String tableName;

  public SQLUpdater() {
  }

  public void initialize(InputSplit split, JobConf jc) throws IOException {
    if (conn != null) {
      return;
    }
    List<String> updateStmts = loadUpdateStatements(split, jc);
    for (String sql : updateStmts) {
      String[] pieces = sql.toUpperCase(Locale.ENGLISH).split("\\s+");
      String table = null;
      if ("UPDATE".equals(pieces[0])) {
        table = pieces[1];
      } else if ("DELETE".equals(pieces[0])) {
        table = pieces[2];
      } else {
        throw new IllegalStateException("Unsupported SQL DML statement: " + sql);
      }
      if (this.tableName == null) {
        this.tableName = table;
      } else if (!tableName.equals(table)) {
        throw new IllegalStateException("Multiple table names in DDL: " + tableName + " and " + table);
      }
    }

    if (tableName != null) {
      this.helper = createDMLHelper(tableName, split, jc);
      this.updateStmts = Lists.newArrayList();

      try {
        this.conn = DriverManager.getConnection("jdbc:h2:mem:");
      } catch (SQLException e) {
        throw new RuntimeException("H2 database not found", e);
      }

      try {
        this.helper.initialize(conn);
        for (String sql : updateStmts) {
          this.updateStmts.add(conn.prepareStatement(sql));
        }
      } catch (SQLException e) {
        throw new IOException("Could not execute DDL", e);
      }
    }
  }

  protected abstract DMLHelper<K, V> createDMLHelper(String tableName, InputSplit split, JobConf jc);

  private long asSCDTime(String text, long defaultValue) {
    if (text == null || text.isEmpty()) {
      return defaultValue;
    } else {
      try {
        return Long.valueOf(text);
      } catch (NumberFormatException e) {
        return ISODateTimeFormat.dateOptionalTimeParser().parseMillis(text);
      }
    }
  }

  private List<String> loadUpdateStatements(InputSplit split, JobConf jc) throws IOException {
    long currentSCDTime = asSCDTime(jc.get("scd.time", ""), System.currentTimeMillis());
    List<String> stmts = Lists.newArrayList();
    if (split instanceof FileSplit) {
      Path base = ((FileSplit) split).getPath();
      FileSystem fs = base.getFileSystem(jc);
      Path updates = new Path(base.getParent(), ".updates");
      if (fs.exists(updates)) {
        stmts.addAll(readLines(fs, updates, currentSCDTime));
      }
    }
    //TODO: get updates from distributed cache
    return stmts;
  }

  private static final String TIME_PREFIX = "-- time=";

  private List<String> readLines(FileSystem fs, Path path, long rootScdTime) throws IOException {
    List<String> lines = Lists.newArrayList();
    long currentScdTime = 0L;
    for (String line : CharStreams.readLines(new InputStreamReader(fs.open(path)))) {
      if (line.toLowerCase(Locale.ENGLISH).startsWith(TIME_PREFIX)) {
        currentScdTime = asSCDTime(line.substring(TIME_PREFIX.length()), rootScdTime);
      } else if (currentScdTime <= rootScdTime) {
        lines.add(line);
      }
    }
    return lines;
  }

  public boolean apply(K currentKey, V currentValue) {
    if (tableName == null) {
      return false;
    }
    try {
      helper.insertValues(currentKey, currentValue);
      for (PreparedStatement ps : updateStmts) {
        ps.execute();
      }
      return helper.retrieveResults(currentKey, currentValue);
    } catch (SQLException e) {
      e.printStackTrace();
      return true;
    }
  }

  public void close() {
    try {
      if (helper != null) {
        helper.close();
      }
      if (updateStmts != null) {
        for (PreparedStatement ps : updateStmts) {
          ps.close();
        }
      }
      if (conn != null) {
        conn.close();
      }
    } catch (SQLException e) {
      // NBD, but log these
    }
    conn = null;
  }
}
