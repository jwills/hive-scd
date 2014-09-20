package com.cloudera.hive.scd;

import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;

public abstract class SQLUpdater<K, V> {

  private Connection conn;
  private DMLHelper<K, V> helper;
  private List<PreparedStatement> updateStmts;
  private String tableName;

  public SQLUpdater() {
    try {
      Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Could not load embedded derby driver", e);
    }
  }

  public void initialize(InputSplit split, JobConf jc) throws IOException {
    if (conn != null) {
      return;
    }
    try {
      this.conn = DriverManager.getConnection("jdbc:derby:scd;create=true");
    } catch (SQLException e) {
      throw new RuntimeException("Derby not found", e);
    }
    List<String> updateStmts = loadUpdateStatements(split, jc);
    boolean valid = true;
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
    this.helper = createDMLHelper(tableName, split, jc);
    this.updateStmts = Lists.newArrayList();

    try {
      this.helper.initialize(conn);
      for (String sql : updateStmts) {
        this.updateStmts.add(conn.prepareStatement(sql));
      }
    } catch (SQLException e) {
      throw new IOException("Could not execute DDL", e);
    }
  }

  protected abstract DMLHelper<K, V> createDMLHelper(String tableName, InputSplit split, JobConf jc);

  private List<String> loadUpdateStatements(InputSplit split, JobConf jc) throws IOException {
    List<String> stmts = Lists.newArrayList();
    if (split instanceof FileSplit) {
      Path base = ((FileSplit) split).getPath();
      FileSystem fs = base.getFileSystem(jc);
      Path updates = new Path(base.getParent(), ".updates");
      if (fs.exists(updates)) {
        stmts.addAll(readLines(fs, updates));
      }
      Path parentUpdates = new Path(base.getParent().getParent(), ".updates");
      if (fs.exists(parentUpdates)) {
        stmts.addAll(readLines(fs, parentUpdates));
      }
    }
    //TODO: get updates from distributed cache
    return stmts;
  }

  private List<String> readLines(FileSystem fs, Path path) throws IOException {
    return CharStreams.readLines(new InputStreamReader(fs.open(path)));
  }

  public boolean apply(K currentKey, V currentValue) {
    try {
      helper.insertValues(currentKey, currentValue);
      for (PreparedStatement ps : updateStmts) {
        ps.execute();
      }
      return helper.retrieveResults(currentKey, currentValue);
    } catch (SQLException e) {
      //TODO: log this
      return false;
    }
  }

  public void close() {
    try {
      helper.close();
      for (PreparedStatement ps : updateStmts) {
        ps.close();
      }
      conn.close();
    } catch (SQLException e) {
      // NBD, but log these
    }
    conn = null;
  }
}
