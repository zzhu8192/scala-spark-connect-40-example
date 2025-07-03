# Spark Connect 4.x Example Project

[![Java 21](https://img.shields.io/badge/Java-21%20LTS-orange.svg)](https://adoptium.net/)
[![Scala 2.13](https://img.shields.io/badge/Scala-2.13.18-red.svg)](https://www.scala-lang.org/)
[![Apache Spark 4.2.1](https://img.shields.io/badge/Apache%20Spark-4.2.1-blue.svg)](https://spark.apache.org/)
[![Delta Lake 4.2.0](https://img.shields.io/badge/Delta%20Lake-4.2.0-00ADD8.svg)](https://delta.io/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)

A production-ready Maven skeleton template for **Apache Spark Connect 4.x** applications written in **Scala 2.13** running on **Java 21 (LTS)**. This project demonstrates how to build and test thin-client Spark applications decoupled from the Spark execution engine using the **Spark Connect API** (`spark-connect-client-jvm`), integrating **Delta Lake 4.2.0**, remote Hive metastore support, unit test utilities, and local server startup workflows.

---

## Table of Contents

- [Overview & Architectural Intent](#overview--architectural-intent)
- [Spark Connect vs. Classic In-Process Spark](#spark-connect-vs-classic-in-process-spark)
- [Key Features](#key-features)
- [Tech Stack](#tech-stack)
- [System Requirements](#system-requirements)
- [Prerequisites & Installation](#prerequisites--installation)
- [Running the Spark Connect Server Locally](#running-the-spark-connect-server-locally)
- [Delta Lake Integration Guide](#delta-lake-integration-guide)
- [Hive Metastore with Remote Derby Setup](#hive-metastore-with-remote-derby-setup)
- [Building the Project](#building-the-project)
- [Running the Application](#running-the-application)
- [Running Unit Tests](#running-unit-tests)
- [Running & Debugging in IntelliJ IDEA](#running--debugging-in-intellij-idea)
- [Troubleshooting & Common Errors](#troubleshooting--common-errors)
- [Project Structure](#project-structure)
- [Contributing](#contributing)
- [License](#license)

---

## Overview & Architectural Intent

**Spark Connect** (introduced in Apache Spark 3.4 and mature in Spark 4.x) fundamentally changes how client applications interact with Apache Spark:

```
+--------------------------------------------------------+
|                   Client Application                   |
|   (Scala 2.13 / Java 21 / spark-connect-client-jvm)    |
|   - Thin JVM process                                   |
|   - Constructs DataFrame & SQL expressions             |
|   - Serializes unresolved logical plans via Protobuf  |
+---------------------------+----------------------------+
                            |
                            | gRPC (sc://localhost:15002)
                            v
+--------------------------------------------------------+
|               Spark Connect Server                     |
|   (Bundled sbin/start-connect-server.sh)               |
|   - Receives logical plans via gRPC                    |
|   - Optimizes execution via Catalyst Optimizer         |
|   - Manages DAG scheduling, Delta Lake, & Hive Catalog |
|   - Streams query results back via Apache Arrow        |
+---------------------------+----------------------------+
                            |
           +----------------+----------------+
           |                                 |
           v                                 v
   +---------------+                 +---------------+
   | Spark Executor|                 | Spark Executor|
   +---------------+                 +---------------+
```

### Why Spark Connect?

1. **Thin, Decoupled Clients**: Your application does not need to embed or manage a heavy in-process Spark cluster (`SparkContext`). The client footprint is tiny and lightweight.
2. **Zero Classpath Collisions**: Dependency hell between your application libraries (e.g., Jackson, Guava, Netty, Protobuf) and Spark/Hadoop internal runtime jars is completely eliminated.
3. **Instant Startup**: No waiting for Spark driver initialization, JVM memory reservation, or local executor spawning when running client code or starting test suites.
4. **Remote & Polyglot Execution**: The same Spark Connect server can serve requests from Scala clients, Python clients (`pyspark`), notebooks, or microservices simultaneously over gRPC.

---

## Spark Connect vs. Classic In-Process Spark

This repository is designed as the Spark Connect counterpart to [scala-spark40-example](https://github.com/zzhu8192/scala-spark40-example):

| Dimension | Classic Spark (`scala-spark40-example`) | Spark Connect (`spark40connect`) |
|:---|:---|:---|
| **Client Model** | Fat driver process embedded in client JVM | Thin JVM client (`spark-connect-client-jvm`) |
| **Communication** | Internal in-memory JVM calls / Akka / Netty | Standardized gRPC over port `15002` |
| **Engine Startup** | Spawns Spark driver & executors inside process | Connects to an existing Spark Connect Server |
| **Dependency Scope** | `spark-core`, `spark-sql`, `spark-hive` bundled | Lightweight `spark-connect-client-jvm` |
| **Data Transfer** | In-memory JVM objects (`InternalRow`, `RDD`) | Arrow-encoded record batches streamed over gRPC |
| **UDF / Code Upload**| Direct classloader access | Uploaded to server via `spark.addArtifact(...)` |

---

## Key Features

- **Apache Spark 4.2.1 & Spark Connect**: Uses official `spark-connect-client-jvm_2.13:4.2.1`.
- **Delta Lake 4.2.0 Integration**: Demonstrates Delta Lake catalog wiring and server-side ACID table capabilities over Spark Connect.
- **Java 21 (LTS) & Scala 2.13.18**: Built for modern LTS Java runtimes with Java 17 bytecode compatibility flags (`-release 17`).
- **Dynamic Connection Management**: Client dynamically targets `SPARK_CONNECT_URL` (default: `sc://localhost:15002`).
- **Sample Configuration Templates** in [`conf/`](conf/):
  - [`conf/spark-defaults.conf.template`](conf/spark-defaults.conf.template): Server-side gRPC port, Arrow batch sizes, and Delta catalog configurations.
  - [`conf/core-site.xml.template`](conf/core-site.xml.template): Remote Derby Network Server connection for shared Hive metastore access.
- **Spark Connect Unit Testing (`TestSparkSession`)**:
  - Reusable test trait using `ScalaTest` and `JUnit 4`.
  - Dynamically points to local or remote Spark Connect server.
  - Support for `sparkSession.addArtifact(...)` to ship local client JARs to executors for custom Dataset `map` functions and UDFs.
- **Cross-Platform Execution Scripts**: Includes `./run.sh` (Linux/macOS) and `run.bat` (Windows).

---

## Tech Stack

| Component | Version | Notes |
|:---|:---|:---|
| **Java** | 21 (LTS) | Built with Java 17 bytecode compatibility (`-release 17`) |
| **Scala** | 2.13.18 | Modern Scala 2.13 series |
| **Apache Spark** | 4.2.1 | `spark-connect-client-jvm_2.13` |
| **Delta Lake** | 4.2.0 | `delta-spark_2.13` (server-side extension) |
| **Apache Derby** | 10.16.1.1 | `derbyclient` (network client for Hive metastore) |
| **Apache Maven** | 3.9.9+ | Build and dependency orchestration |
| **ScalaTest** | 3.2.19 | Unit testing framework |
| **JUnit** | 4.13.2 | Test runner integration (`JUnitRunner`) |
| **JaCoCo** | 0.8.12 | Code coverage metrics |

---

## System Requirements

| Tier | RAM | CPU Cores | Storage | Experience |
|:---|:---|:---|:---|:---|
| **Recommended** | 16 GB - 32 GB | 8+ cores | SSD | Fast compilation, concurrent server + client, snappy tests |
| **Minimum** | 8 GB - 16 GB | 4 cores | SSD | Functional for local development and test execution |
| **Not Recommended** | < 8 GB | 2 cores | HDD | High risk of compilation timeouts or slow JVM performance |

---

## Prerequisites & Installation

### 1. Java 21 (LTS)
Install an OpenJDK 21 distribution, such as [Eclipse Temurin](https://adoptium.net/):
- **Linux (Ubuntu/Debian)**:
  ```bash
  sudo apt-get update && sudo apt-get install -y openjdk-21-jdk
  ```
- **macOS (Homebrew)**:
  ```bash
  brew install openjdk@21
  ```
- Verify:
  ```bash
  java -version
  ```

### 2. Apache Maven
- Install Maven 3.9+ via package manager or from [maven.apache.org](https://maven.apache.org/download.cgi).
- Verify:
  ```bash
  mvn -version
  ```

### 3. Apache Spark 4.x Distribution
Download and extract Apache Spark 4.x (built for Hadoop 3 and Scala 2.13):
```bash
wget https://archive.apache.org/dist/spark/spark-4.0.1/spark-4.0.1-bin-hadoop3.tgz
tar -xvzf spark-4.0.1-bin-hadoop3.tgz
export SPARK_HOME="$(pwd)/spark-4.0.1-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
```

---

## Running the Spark Connect Server Locally

Spark Connect requires a running Spark Connect Server to handle client requests. Spark bundles dedicated scripts in `$SPARK_HOME/sbin/`:

### Starting the Server

Run the bundled `start-connect-server.sh` script, supplying the necessary Maven packages for Spark Connect and Delta Lake:

```bash
${SPARK_HOME}/sbin/start-connect-server.sh \
  --packages org.apache.spark:spark-connect_2.13:4.2.1,io.delta:delta-spark_2.13:4.2.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.connect.grpc.binding.port=15002"
```

#### Key Parameters Explained:
- `--packages org.apache.spark:spark-connect_2.13:4.2.1,io.delta:delta-spark_2.13:4.2.0`: Tells the server to download and load the Spark Connect server engine and Delta Lake jars from Maven Central.
- `--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"`: Registers Delta Lake SQL grammar (e.g., `MERGE INTO`, `VACUUM`).
- `--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"`: Sets Delta Lake as the default catalog implementation.
- `--conf "spark.connect.grpc.binding.port=15002"`: Specifies the gRPC listening port (default is `15002`).

> [!TIP]
> **Foreground Mode for Debugging**:
> To run the server in the foreground with console logging instead of daemonizing in the background, pass `--wait` as the first argument:
> ```bash
> ${SPARK_HOME}/sbin/start-connect-server.sh --wait \
>   --packages org.apache.spark:spark-connect_2.13:4.2.1,io.delta:delta-spark_2.13:4.2.0 \
>   --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
>   --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
> ```

### Verifying Server Health

Verify the server is listening on port 15002:

```bash
# Test socket connection
nc -zv localhost 15002

# Inspect recent server logs
tail -f "${SPARK_HOME}/logs/"spark-*-org.apache.spark.sql.connect.service.SparkConnectServer-*.out
```

### Stopping the Server

When finished, gracefully shut down the background server:

```bash
${SPARK_HOME}/sbin/stop-connect-server.sh
```

---

## Delta Lake Integration Guide

In Spark Connect, Delta Lake extensions and catalog rules are executed **server-side**:

1. **Server Configuration**: The Spark Connect Server must be started with Delta Lake packages and extensions (as shown above in the start command).
2. **Client Interaction**: The client uses standard DataFrame and SQL APIs transparently:
   ```scala
   // Write data in Delta format over Spark Connect
   df.write
     .format("delta")
     .mode("overwrite")
     .save("/tmp/delta-table")

   // Read Delta table
   val deltaDf = spark.read
     .format("delta")
     .load("/tmp/delta-table")

   // Delta SQL commands
   spark.sql("VACUUM delta.`/tmp/delta-table` RETAIN 168 HOURS")
   ```

---

## Hive Metastore with Remote Derby Setup

When running the Spark Connect Server against a shared Hive metastore, embedded Derby locks the `metastore_db` folder with a single-process lock (`db.lck`). To allow concurrent access, run Apache Derby as a network service:

1. **Start Derby Network Server** on port 1527:
   ```bash
   export DERBY_HOME=/opt/derby
   ${DERBY_HOME}/bin/startNetworkServer -h 127.0.0.1 -p 1527 &
   ```
2. **Configure Spark Connect Server**:
   Copy [`conf/core-site.xml.template`](conf/core-site.xml.template) to `$SPARK_HOME/conf/core-site.xml` and ensure `derbyclient-10.16.1.1.jar` is in `$SPARK_HOME/jars/`.
3. The Spark Connect Server will persist Hive metadata to the network-backed Derby database, allowing multiple clients to access external tables simultaneously.

---

## Building the Project

Compile and package the project using Maven:

```bash
# Compile Scala and Java sources
mvn clean compile

# Compile test sources
mvn test-compile

# Package client JAR (skipping unit tests if local server is not running)
mvn clean package -DskipTests
```

This generates:
- `target/sparkconnect-1.0.0-SNAPSHOT.jar` (thin client JAR)
- `target/sparkconnect-1.0.0-SNAPSHOT-jar-with-dependencies.jar` (assembly Fat-JAR with non-provided dependencies)

---

## Running the Application

### Option A: Using the Convenience Script (Recommended)

Ensure your Spark Connect Server is running, then run:

```bash
# Linux / macOS
./run.sh

# Windows
run.bat
```

To point to a remote server, specify `SPARK_CONNECT_URL`:
```bash
SPARK_CONNECT_URL="sc://remote-host:15002" ./run.sh
```

### Option B: Using `spark-submit --remote`

```bash
spark-submit \
  --remote "sc://localhost:15002" \
  --class com.uj.example.Example \
  target/sparkconnect-1.0.0-SNAPSHOT.jar \
  file://$(pwd)/src/main/resources/test.csv \
  /tmp/spark-connect-example-output
```

---

## Running Unit Tests

Unit tests in [`ExampleTest.scala`](src/test/scala/com/uj/example/ExampleTest.scala) connect to the Spark Connect Server via [`TestSparkSession.scala`](src/test/scala/com/uj/testutils/TestSparkSession.scala).

1. **Start the Spark Connect Server** (see [Running the Spark Connect Server Locally](#running-the-spark-connect-server-locally)).
2. **Build the project JAR** (required for Dataset `map` artifact shipping):
   ```bash
   mvn package -DskipTests
   ```
3. **Execute Tests**:
   ```bash
   mvn test
   ```
   Or target a custom remote address:
   ```bash
   SPARK_CONNECT_URL="sc://localhost:15002" mvn test
   ```

### Shipping Client Artifacts (`sparkSession.addArtifact`)

When executing operations that require custom client classes on server executors (such as Dataset `map` operations or custom UDFs), Spark Connect requires pushing the client JAR to the server session:

```scala
val localJar = new java.io.File("target/sparkconnect-1.0.0-SNAPSHOT.jar")
if (localJar.exists()) {
  sparkSession.addArtifact(localJar.getAbsolutePath)
}
```

This ensures the remote Spark cluster executors can resolve and execute your compiled Scala classes.

---

## Running & Debugging in IntelliJ IDEA

1. **Open Project**: Open the repository folder in IntelliJ IDEA (select `pom.xml` to import as Maven project).
2. **Set Project SDK**: Navigate to **File > Project Structure > Project**, select **JDK 21**.
3. **Configure Run/Debug Configuration**:
   - Main class: `com.uj.example.Example`
   - Program arguments: `src/main/resources/test.csv /tmp/spark-output`
   - Environment variables: `SPARK_CONNECT_URL=sc://localhost:15002`
   - **Crucial Setting**: Check **"Include dependencies with 'Provided' scope"** in the Run Configuration so IntelliJ puts `spark-connect-client-jvm` on the classpath.
4. Set breakpoints and debug normally without running heavy local Spark drivers.

---

## Troubleshooting & Common Errors

### 1. `io.grpc.StatusRuntimeException: UNAVAILABLE: io exception`
- **Cause**: The client cannot connect to `sc://localhost:15002`.
- **Fix**: Check that the Spark Connect Server is running (`$SPARK_HOME/sbin/start-connect-server.sh`) and that port 15002 is not blocked by a firewall.

### 2. `ClassNotFoundException` on Executor during `map` or UDF
- **Cause**: The remote Spark executors do not possess your local application's compiled bytecode.
- **Fix**: Ensure your test or application calls `sparkSession.addArtifact("path/to/sparkconnect-1.0.0-SNAPSHOT.jar")` prior to executing the stage.

### 3. `DeltaAnalysisException: Table not found / Catalog not supported`
- **Cause**: The Spark Connect Server was started without Delta Lake extensions.
- **Fix**: Ensure `--packages io.delta:delta-spark_2.13:4.2.0` and `--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension` are passed when launching `start-connect-server.sh`.

---

## Project Structure

```
spark40connect/
├── conf/
│   ├── core-site.xml.template           # Sample remote Derby Hive metastore config
│   └── spark-defaults.conf.template     # Sample Spark Connect & Delta server config
├── src/
│   ├── main/
│   │   ├── resources/
│   │   │   └── test.csv                 # Sample input CSV data
│   │   └── scala/
│   │       └── com/uj/example/
│   │           └── Example.scala        # Spark Connect ETL & SQL sample pipeline
│   └── test/
│       └── scala/
│           └── com/uj/
│               ├── example/
│               │   └── ExampleTest.scala        # ScalaTest suite for Spark Connect
│               └── testutils/
│                   └── TestSparkSession.scala   # Spark Connect test harness
├── .editorconfig                        # Code formatting consistency rules
├── .gitignore                           # Git ignore rules for build and IDE files
├── CONTRIBUTING.md                      # Contribution guidelines
├── LICENSE                              # Apache License 2.0
├── pom.xml                              # Maven build file (Java 21, Scala 2.13, Spark 4.2.1)
├── README.md                            # Comprehensive documentation
├── run.bat                              # Windows execution script
└── run.sh                               # Linux / macOS execution script
```

---

## Contributing

Contributions, issues, and feature requests are welcome! See [`CONTRIBUTING.md`](CONTRIBUTING.md) for details on opening pull requests and conventional commit standards.

---

## License

This project is licensed under the [Apache License, Version 2.0](LICENSE).
