# Connector benchmarks

Read and write benchmarks for the CSV, JSON, Excel, Parquet and HTTP connectors. Use them to measure connector
performance work against the committed baselines in [`baselines/`](baselines/).

Every benchmark reads or writes the same 20-column `WideRecord` (integers, longs, doubles, decimals, strings, dates, a
flag and a GUID), so results are comparable across connectors. File connectors use the in-memory storage provider from
`NPipeline.Tests.Common`, and HTTP uses in-process handlers. The numbers therefore measure parsing, mapping and
serialisation, not disk or network I/O.

| Benchmark | Rows | What it measures |
| --- | --- | --- |
| `CsvBenchmarks.Write` / `Read` | 100,000 | CSV sink and source, binding columns to members |
| `JsonBenchmarks.Write` / `Read` | 100,000 | JSON sink and source, for both `Array` and `NewlineDelimited` |
| `ExcelBenchmarks.Write` / `Read` | 25,000 | XLSX sink and source (fewer rows, kept from the baseline, when the writer was slow) |
| `ParquetBenchmarks.Write` / `Read` | 100,000 | Parquet sink and source with default row groups |
| `ParquetBenchmarks.ReadThreeColumns` | 100,000 | Reading 3 of 20 columns into a record, so only those columns are read |
| `ParquetBenchmarks.ReadThreeColumnsManually` | 100,000 | The same with `ProjectedColumns` and a `ParquetRow` mapper |
| `HttpBenchmarks.SourcePaged` | 100,000 | Page-number pagination over 100 root-array pages of 1,000 items |
| `HttpBenchmarks.SourcePagedWrapped` | 100,000 | The same over `{"meta":{"total":N},"data":[…]}` pages, with `ItemsJsonPath` and a total |
| `HttpBenchmarks.SinkBatched` | 100,000 | POSTs in batches of 100 |
| `SqlServerBenchmarks`, `PostgresBenchmarks`, `MySqlBenchmarks`, `DuckDBBenchmarks`: `Read` | 20,000 | The SQL source reading a 20-column table |
| … `WriteBatch` | 20,000 | Multi-row `INSERT` statements (DuckDB: its SQL strategy) |
| … `WriteBulk` | 20,000 | The connector's bulk path: `SqlBulkCopy`, binary `COPY`, `LOAD DATA`, DuckDB's appender |
| `KafkaBenchmarks`, `RabbitMqBenchmarks`, `SqsBenchmarks`, `ServiceBusBenchmarks`: `Publish` | 2,000 (Service Bus 200) | The sink publishing JSON messages with its defaults |
| … `Consume` | 2,000 (Service Bus 200) | The source reading and acknowledging each message from a queue or topic filled beforehand |

## Run

Run from this directory, in Release mode, on a quiet machine:

```shell
dotnet run -c Release -f net10.0 -- --filter '*'
```

To run one connector, filter by class name, for example `--filter '*Parquet*'`. The database benchmarks start SQL
Server, PostgreSQL and MySQL in Docker (Testcontainers, reused between runs), and the messaging benchmarks Kafka,
RabbitMQ, LocalStack and the Service Bus emulator; they run in process, so Docker must be running. Use `-f net8.0` to measure the LTS
target. Results are written to `BenchmarkDotNet.Artifacts/results/` as GitHub markdown and full JSON.

The **Connector Benchmarks** GitHub workflow runs the same command on demand (`workflow_dispatch`) and uploads the
results as an artifact.

## Update the baselines

After a change that is meant to improve performance:

1. Run the affected benchmarks on the same machine as the existing baseline.
2. Compare them with the table in `baselines/`.
3. Add a new baseline file named `YYYY-MM-DD-<commit>.md`. Keep the old one, so the history of gains stays visible.
