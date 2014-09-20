/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.hive.scd.avro;

import com.cloudera.hive.scd.BaseSCDRecordReader;
import com.cloudera.hive.scd.DMLHelper;
import com.cloudera.hive.scd.SQLUpdater;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

public class AvroSCDInputFormat extends AvroContainerInputFormat {
  @Override
  public RecordReader<NullWritable, AvroGenericRecordWritable> getRecordReader(
      InputSplit split, JobConf jc, Reporter reporter) throws IOException {
    RecordReader<NullWritable, AvroGenericRecordWritable> parent = super.getRecordReader(split, jc, reporter);
    AvroSQLUpdater updater = new AvroSQLUpdater();
    updater.initialize(split, jc);
    return new BaseSCDRecordReader<NullWritable, AvroGenericRecordWritable>(parent, updater);
  }

  static class AvroSQLUpdater extends SQLUpdater<NullWritable, AvroGenericRecordWritable> {
    @Override
    protected DMLHelper<NullWritable, AvroGenericRecordWritable> createDMLHelper(
        String tableName, InputSplit split, JobConf jc) {
      return new AvroDMLHelper(tableName, getSchema(split, jc));
    }

    private Schema getSchema(InputSplit split, JobConf job) {
      // Inside of a MR job, we can pull out the actual properties
      if(AvroSerdeUtils.insideMRJob(job)) {
        MapWork mapWork = Utilities.getMapWork(job);

        // Iterate over the Path -> Partition descriptions to find the partition
        // that matches our input split.
        for (Map.Entry<String,PartitionDesc> pathsAndParts: mapWork.getPathToPartitionInfo().entrySet()){
          String partitionPath = pathsAndParts.getKey();
          if(pathIsInPartition(((FileSplit)split).getPath(), partitionPath)) {
            if(LOG.isInfoEnabled()) {
              LOG.info("Matching partition " + partitionPath +
                  " with input split " + split);
            }

            Properties props = pathsAndParts.getValue().getProperties();
            if(props.containsKey(AvroSerdeUtils.SCHEMA_LITERAL) || props.containsKey(AvroSerdeUtils.SCHEMA_URL)) {
              try {
                return AvroSerdeUtils.determineSchemaOrThrowException(props);
              } catch (Exception e) {
                throw new RuntimeException("Avro serde exception", e);
              }
            }
            else {
              return null; // If it's not in this property, it won't be in any others
            }
          }
        }
        if(LOG.isInfoEnabled()) {
          LOG.info("Unable to match filesplit " + split + " with a partition.");
        }
      }

      // In "select * from table" situations (non-MR), we can add things to the job
      // It's safe to add this to the job since it's not *actually* a mapred job.
      // Here the global state is confined to just this process.
      String s = job.get(AvroSerdeUtils.AVRO_SERDE_SCHEMA);
      if(s != null) {
        LOG.info("Found the avro schema in the job: " + s);
        return Schema.parse(s);
      }
      // No more places to get the schema from. Give up.  May have to re-encode later.
      return null;
    }

    private boolean pathIsInPartition(Path split, String partitionPath) {
      boolean schemeless = split.toUri().getScheme() == null;
      if (schemeless) {
        String schemelessPartitionPath = new Path(partitionPath).toUri().getPath();
        return split.toString().startsWith(schemelessPartitionPath);
      } else {
        return split.toString().startsWith(partitionPath);
      }
    }
  }

  static class AvroDMLHelper implements DMLHelper<NullWritable, AvroGenericRecordWritable> {

    private final String tableName;
    private final Schema schema;

    private Connection conn;
    private PreparedStatement deleteStmt;
    private PreparedStatement insertStmt;

    public AvroDMLHelper(String tableName, Schema schema) {
      this.tableName = tableName;
      this.schema = schema;
    }

    @Override
    public void initialize(Connection conn) throws SQLException {
      this.conn = conn;
      StringBuilder create = new StringBuilder("CREATE TABLE ").append(tableName).append(" (");
      StringBuilder insert = new StringBuilder("INSERT INTO ").append(tableName).append(" VALUES (");
      for (int i = 0; i < schema.getFields().size(); i++) {
        Schema.Field sf = schema.getFields().get(i);
        create.append(sf.name()).append(' ').append(getSQLType(sf.schema())).append(',');
        insert.append("?,");
      }
      create.deleteCharAt(create.length() - 1).append(")");
      insert.deleteCharAt(insert.length() - 1).append(")");

      System.out.println("Create is: " + create.toString());
      System.out.println("Insert is: " + insert.toString());

      conn.createStatement().execute(create.toString());
      insertStmt = conn.prepareStatement(insert.toString());
      deleteStmt = conn.prepareStatement("DELETE FROM " + tableName);
    }

    private static Map<Schema.Type, String> SQL_TYPES = ImmutableMap.<Schema.Type, String>builder()
        .put(Schema.Type.BOOLEAN, "BOOLEAN")
        .put(Schema.Type.DOUBLE, "DOUBLE")
        .put(Schema.Type.FLOAT, "REAL")
        .put(Schema.Type.INT, "INTEGER")
        .put(Schema.Type.LONG, "BIGINT")
        .put(Schema.Type.STRING, "VARCHAR(32672)")
        .build();

    static String getSQLType(Schema schema) {
      if (SQL_TYPES.containsKey(schema.getType())) {
        return SQL_TYPES.get(schema.getType());
      }
      if (schema.getType() == Schema.Type.UNION) {
        if (schema.getTypes().size() == 2) {
          if (schema.getTypes().get(0).getType() == Schema.Type.NULL) {
            return getSQLType(schema.getTypes().get(1));
          } else if (schema.getTypes().get(1).getType() == Schema.Type.NULL) {
            return getSQLType(schema.getTypes().get(0));
          }
        }
      }
      throw new UnsupportedOperationException("Unsupported Avro type: " + schema.getType());
    }

    @Override
    public void insertValues(NullWritable key, AvroGenericRecordWritable value) throws SQLException {
      boolean dret = deleteStmt.execute();
      System.out.println("Delete executed with = " + dret);
      for (int i = 0; i < schema.getFields().size(); i++) {
        insertStmt.setObject(i + 1, convertIn(value.getRecord().get(i)));
      }
      boolean ret = insertStmt.execute();
      System.out.println("Insert executed w/return value = " + ret);
      System.out.println("Inserting record: " + value.getRecord().toString());
    }

    private Object convertIn(Object v) {
      if (v instanceof Utf8) {
        return v.toString();
      }
      return v;
    }

    private Object convertOut(Object v) {
      if (v instanceof String) {
        return new Utf8((String) v);
      }
      return v;
    }

    @Override
    public boolean retrieveResults(NullWritable key, AvroGenericRecordWritable value) throws SQLException {
      Statement stmt = conn.createStatement();
      try {
        ResultSet rs = stmt.executeQuery("select * from " + tableName);
        if (rs.next()) {
          for (int i = 0; i < schema.getFields().size(); i++) {
            value.getRecord().put(i, convertOut(rs.getObject(i + 1)));
          }
          return false;
        } else {
          return true;
        }
      } finally {
        if (stmt != null) {
          stmt.close();
        }
      }
    }

    @Override
    public void close() throws SQLException {
      if (deleteStmt != null) {
        deleteStmt.close();
        deleteStmt = null;
      }
      if (insertStmt != null) {
        insertStmt.close();
        insertStmt = null;
      }
    }
  }
}
