# Hive SCD: A New Type of Slowly-Changing Dimension

In data warehousing, [slowly changing dimensions](http://en.wikipedia.org/wiki/Slowly_changing_dimension) (SCDs) are dimension tables that are updated at irregular intervals. Slowly changing dimensions
are difficult to handle in Apache Hive because the underlying Hadoop File System is append-only,
which means that any changes to existing records require re-writing entire files. As of
version 0.13, Hive does not support classic SQL DML like UPDATE or DELETE, although there
is [work on-going](https://issues.apache.org/jira/browse/HIVE-5317) to add this functionality
to Hive at some point in the future.

There are three main types of SCDs:

1. *Type 1*: Overwrite the value of a field when it changes.
2. *Type 2*: Adds a version number or effective date column for each record and have multiple rows for the same natural key.
3. *Type 3*: Adds a separate history column to each row with a limited number of older values.

Both Type 1 and Type 3 SCDs always require some kind of update operation, and Type 2 SCDs that
use the effective date strategy also require updates. There are also a few other SCD patterns
that are variants on the above, such as the Type 6 (1+2+3) SCD.

This project explores building a new type of slowly changing dimension, which I'll call
*Type 7*. The idea is that we store a list of SQL DML statements (UPDATEs and DELETEs)
in a file in the same directory as the underlying data files, and we apply those DML statements
as we read the data into Hive in order to modify and/or remove records in the dimension
table. The interesting bit is that we can mark each of the DML statements with a timestamp
to indicate the time that it becomes effective, and then roll time forward or backward in
the client in order to decide which DML statements to apply to the data.

# Build and Install

Right now, this project only supports Avro-based files in Hive, the kind that are
currently read using `org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat`.

Build and install is pretty straightforward and should work with any Hive version after
0.10.0. To build, run:

	mvn clean package

This will create a JAR file in the `target` directory called
`hive-scd-0.1.0-jar-with-dependencies.jar`. You'll probably want to copy that JAR file
to an edge node of your Hadoop cluster with a shorter name, like `hive-scd.jar`. If you
find that you really enjoy using the JAR file to work with SCDs, then you can copy the
JAR file into Hive's `lib/` directory on each node of your cluster, but in the example
below we'll assume that we're just using the JAR from the client.

The Hive DDL for the SCD table should be exactly like the DDL for any other Avro-based
table, except the `INPUTFORMAT` should be `com.cloudera.hive.scd.avro.AvroSCDInputFormat`.

# Example

The `examples/` directory contains some data and HiveQL that we'll use to illustrate the
use of the `AvroSCDInputFormat`.

## Setup

Start the Hive shell and load the hive-scd.jar file we created above, or use the
one in the `example` directory, which should work against most modern versions of Hive.

	ADD JAR hive-scd.jar;

Now, let's create the `doctors` table in Hive and load in the data from the `doctors.avro`
file. The HQL for this is contained inside of the `doctors.hql` file for easy reference.
You'll note that we're embedding the Avro schema in the table properties; this isn't usually
a great idea, but it'll do for this example.

	CREATE TABLE doctors
	ROW FORMAT
	SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
	STORED AS
	INPUTFORMAT 'com.cloudera.hive.scd.avro.AvroSCDInputFormat'
	OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
	TBLPROPERTIES ('avro.schema.literal'='{
	  "namespace": "testing.hive.avro.serde",
	  "name": "doctors",
	  "type": "record",
	  "fields": [
	    {
	      "name":"number",
	      "type":"int",
	      "doc":"Order of playing the role"
	    },
	    {
	      "name":"first_name",
	      "type":"string",
	      "doc":"first name of actor playing role"
	    },
	    {
	      "name":"last_name",
	      "type":"string",
	      "doc":"last name of actor playing role"
	    },
	    {
	      "name":"extra_field",
	      "type":"string",
	      "doc:":"an extra field not in the original file",
	      "default":"fishfingers and custard"
	    }
	  ]
	}');
	LOAD DATA LOCAL INPATH 'doctors.avro' INTO TABLE doctors;

We can now display the contents of the table, which looks like this:

	hive> SELECT * from doctors;
	OK
	6	Colin	Baker	fishfingers and custard
	3	Jon	Pertwee	fishfingers and custard
	4	Tom	Baker	fishfingers and custard
	5	Peter	Davison	fishfingers and custard
	11	Matt	Smith	fishfingers and custard
	1	William	Hartnell	fishfingers and custard
	7	Sylvester	McCoy	fishfingers and custard
	8	Paul	McGann	fishfingers and custard
	2	Patrick	Troughton	fishfingers and custard
	9	Christopher	Eccleston	fishfingers and custard
	10	David	Tennant	fishfingers and custard
	Time taken: 1.636 seconds, Fetched: 11 row(s)

This is the raw contents of the Avro file, with no modifications applied.
Note a couple of records in particular: the first row with the doctor named
Colin Baker, and the entry for Patrick Troughton, whose number is equal to 2.

## Defining the Updates

The updates that we would like to apply are defined in the `updates` file in
the `examples` directory. Here are the contents of the file:

	UPDATE doctors set number = 12 where number = 2;
	-- time=2014-09-01
	DELETE FROM doctors WHERE first_name = 'Colin';

As you can see, this is ordinary DML, where the column names in the updates match
the column names of the table (in this case, the names of the fields in the Avro
record.) The name of the table in the DML is actually irrelevant, because we will apply
these updates based on the directory that contains the updates file, but the name of
the table must be consistent in all of the statements.

The line before the DELETE statement is a specially formatted SQL comment that is
used to set the timestamp of all of the statements that come after it. In this case,
it says that the DELETE statement is valid as of midnight on September 1st, 2014.
The format of the value in the `-- time=<value>` expression can either be a long
(representing time since the Java epoch in milliseconds), or a JodaTime datetime
format, like 'yyyy-MM-dd' or 'yyyy-mm-ddTHH:mm:ss' with an optional timezone
offset. If no `-- time=<value>` is specified, the updates are assumed to be valid
since the start of the epoch (i.e., time=0).

To enable the updates, copy the contents of the updates file to a file named `.updates`
in the HDFS directory that contains the data for the doctors table:

	hadoop fs -put updates /user/hive/warehouse/doctors/.updates

Now, when we query the doctors table from Hive, we'll see this:

	hive> select * from doctors;
	OK
	3	Jon	Pertwee	fishfingers and custard
	4	Tom	Baker	fishfingers and custard
	5	Peter	Davison	fishfingers and custard
	11	Matt	Smith	fishfingers and custard
	1	William	Hartnell	fishfingers and custard
	7	Sylvester	McCoy	fishfingers and custard
	8	Paul	McGann	fishfingers and custard
	12	Patrick	Troughton	fishfingers and custard
	9	Christopher	Eccleston	fishfingers and custard
	10	David	Tennant	fishfingers and custard
	Time taken: 2.117 seconds, Fetched: 10 row(s)

As you can see, the updates have been applied to the table: the entry for Colin Baker
has been deleted, and the number for Patrick Troughton has been changed to
12 from 2. These updates will also be applied to any MapReduce jobs that Hive launches
over this table.

We can control which updates are applied to the data by changing the value of the
`scd.time` configuration parameter in Hive. We can set this parameter using either
long timestamp values or JodaTime dates, just as we did in the `.updates` file. For
example, if we set the time to be January 1st, 2014, the DELETE won't be applied
and the entry for Colin Baker will be returned:

	hive> set scd.time=2014-01-01;
	hive> select * from doctors;
	OK
	6	Colin	Baker	fishfingers and custard
	3	Jon	Pertwee	fishfingers and custard
	4	Tom	Baker	fishfingers and custard
	5	Peter	Davison	fishfingers and custard
	11	Matt	Smith	fishfingers and custard
	1	William	Hartnell	fishfingers and custard
	7	Sylvester	McCoy	fishfingers and custard
	8	Paul	McGann	fishfingers and custard
	12	Patrick	Troughton	fishfingers and custard
	9	Christopher	Eccleston	fishfingers and custard
	10	David	Tennant	fishfingers and custard
	Time taken: 0.304 seconds, Fetched: 11 row(s)

In this example, the UPDATE to Patrick Troughton is still applied. If we want to
disable it as well to return the contents of the raw file, we can set the value of
scd.time to -1:

	hive> set scd.time=-1;
	hive> select * from doctors;
	OK
        6       Colin   Baker   fishfingers and custard
        3       Jon     Pertwee fishfingers and custard
        4       Tom     Baker   fishfingers and custard
        5       Peter   Davison fishfingers and custard
        11      Matt    Smith   fishfingers and custard
        1       William Hartnell        fishfingers and custard
        7       Sylvester       McCoy   fishfingers and custard
        8       Paul    McGann  fishfingers and custard
        2       Patrick Troughton       fishfingers and custard
        9       Christopher     Eccleston       fishfingers and custard
        10      David   Tennant fishfingers and custard
        Time taken: 0.269 seconds, Fetched: 11 row(s)

If we reset the scd.time by setting its value to the empty string, the
system uses the current time in milliseconds as the value. This means that we
can also set scd.time to be some time in the future in order to test out new
updates that won't be applied to normal users of the table.

# FAQ

### So how does this work? ###

We use an in-memory [H2](http://www.h2database.com/) database to create a table, load
the Avro records into the table as they are read in, execute the DML statements against
the rows, and then retrieve the contents of the table, update the underlying Avro records
with any values that have changed, and then pass them on to the rest of the Hive query.
If a record is deleted by the DML, then we simply skip it.

### Isn't that, like, horribly inefficient? ###

Yes! But that's ok: CPUs are really fast and are getting faster all the time. Very
few Hive queries are CPU-bound, so the extra overhead of performing the DML in-memory
with H2 isn't that big of a deal. That said, there are definitely some ways to be
more clever with how the updates are distributed to the nodes (e.g., using the distributed
cache instead of reading them directly from HDFS), and we could arguably get clever
about deciding which updates need to actually be performed depending on which columns
are being read, but that's the sort of thing we can worry about later.

In general, you'd probably want to keep a "current" snapshot of the dimension table that
was stored in an optimized columnar format like [Parquet](http://parquet.incubator.apache.org/)
for fast reads, and use the Type 7-enabled "raw" data in order to perform historical queries
against previous versions of the table or to test new updates. You may also want to
periodically compact the updates into a new version of the dimension table when you don't
expect to need to be able to roll any of the updates back.

### Are there any limitations I should be aware of? ###

Yes: right now, type support is pretty limited; we can handle flat Avro records
(so no nested structure) composed of ints, longs, floats, doubles, booleans, and
strings (which are mapped to VARCHAR(32767) values in H2). I hope that this isn't
a major problem for most Avro-serialized dimension tables, although adding decimal
support is probably worthwhile in the near future.

### Will any other file formats be supported in the future? ###

Quite possibly; I chose Avro for the prototype because it was relatively easy to do
and is broadly supported. Shoot me an email or open an issue for other formats you'd
like to see in here.
