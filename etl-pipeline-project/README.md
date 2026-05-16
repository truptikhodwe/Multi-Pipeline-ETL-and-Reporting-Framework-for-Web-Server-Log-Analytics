# Multi-Pipeline ETL Framework

This project is a CLI-based unified ETL reporting framework designed to perform data processing workflows across Apache Hadoop MapReduce, Apache Hive, Apache Pig, and MongoDB pipelines.

## ⚠️ Important Setup Instructions: Update Environment Paths

If you have just cloned or pulled this project from GitHub, **you MUST update the system paths** in the Java source files to point to your local installation directories for Hadoop, Hive, and Pig. 

The current code contains hard-coded absolute paths specific to the original developer's system. The application will fail to launch processes if these paths are not updated.

Please locate and update the `HADOOP_HOME`, `HIVE_HOME`, and `PIG_HOME` constant string variables in the following files to match your local setup:

1. `src/main/java/com/etl/Main.java`
2. `src/main/java/com/etl/controller/MapReducePipeline.java`
3. `src/main/java/com/etl/controller/HivePipeline.java`
4. `src/main/java/com/etl/controller/PigPipeline.java`
5. `src/main/java/com/etl/controller/MongoDBPipeline.java`

**Example:**
Change:
```java
private static final String HADOOP_HOME = "/home/priyanshu-tiwari/hadoop";
```
To:
```java
private static final String HADOOP_HOME = "/usr/local/hadoop"; // Or wherever it is installed
```

## Setup & Execution

1. Make sure your PostgreSQL database is running and the `etl_project` database has been created with the schema from `schema.sql`.
2. Ensure Hadoop (HDFS/YARN), Hive Server, and MongoDB are running.
3. Build and execute the application:

```bash
mvn clean package
./run.sh
```
