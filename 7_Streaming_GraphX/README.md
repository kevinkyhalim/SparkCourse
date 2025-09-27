# Spark Streaming, Structured Steraming and GraphX

## Spark Streaming
- Analyzes continual streams of data
    - Processing log data from a website / server
- Can take data fed to some port, AMazon Kinesis, HDFS, Kafka, Flume
- "Micro-batch" processing
    - 100 ms latency
    - Exactly-once fault tolerance guarantees
- Continuous processing
    - 1 ms latency
    - At-least-one guarantees
    - Mostly for Kafka
- Data stream as an unbounded Input Table, where the Dataframe keeps expanding

### Window Operations
- Looking back over some period of time
- The "slide interval" defines how often we evaluate a window

### Streaming Formats
- Socket
    - Useful for testing only; has no fault tolerance

- Rate
    - Useful for testing only
    - Simulates data arriving at a consistent rate of rows / second
        - optionally can simulate "ramp up time" / when you expect more data to arrive after some time
    - Also has a "rate per micro-batch" source

- File
    - Reads files in a directory as a stream (could be in S3)
    - Processsed in order of modification time
    - Text, CSV, JSON, ORC, Parquet format

- Kafka

## Joining Streams
- Example of joining orders with payments

- Watermarks : a bound on how much is eligible to be joined aka how old a given column can be before the row is discarded
- Triggers : how much data is accumulated prior to processing
    - Fixed Intervals where we process after a time window (equivalent of micro-batches)
    - Continuous, only works with supported sources & sinks (i.e. Kafka) and has no aggregate SQL functions and no functions based on current time
    - File Sources: Set max number of new files per batch or max bytes
    - Kafka : use maxOffsetsPerTrigger

## GraphX
- Currently SCALA only, and python suport doesn't appear to be forthcoming
- Only useful for specific things, can measure things such as "connectedness", degree distirbution, average path length, triangle counts
- Can only join graphs together and transform graphs quickly