// ReSharper disable ClassNeverInstantiated.Local

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using NPipeline.Benchmarks.Common;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Execution;
using NPipeline.Execution.Factories;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using static NPipeline.Benchmarks.Common.BenchmarkDataGenerators;

namespace NPipeline.Benchmarks.Benchmarks;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class RealWorldWorkloadBenchmarks
{
    private PipelineContext _ctx = null!;
    private PipelineRunner _runner = null!;

    [GlobalSetup]
    public void Setup()
    {
        _runner = new PipelineRunner(new PipelineFactory(), new DefaultNodeFactory());
        _ctx = PipelineContext.Default;
    }

    [Benchmark]
    [Arguments(1_000)]
    [Arguments(10_000)]
    [Arguments(100_000)]
    public async Task CsvProcessingBenchmark(int recordCount)
    {
        _ctx.Parameters["recordCount"] = recordCount;

        await _runner.RunAsync<CsvProcessingPipeline>(_ctx);
    }

    [Benchmark]
    [Arguments(1_000)]
    [Arguments(10_000)]
    [Arguments(100_000)]
    public async Task JsonProcessingBenchmark(int recordCount)
    {
        _ctx.Parameters["recordCount"] = recordCount;

        await _runner.RunAsync<JsonProcessingPipeline>(_ctx);
    }

    [Benchmark]
    [Arguments(1_000)]
    [Arguments(10_000)]
    public async Task CsvWithParallelProcessingBenchmark(int recordCount)
    {
        _ctx.Parameters["recordCount"] = recordCount;

        await _runner.RunAsync<CsvParallelProcessingPipeline>(_ctx);
    }

    [Benchmark]
    [Arguments(1_000)]
    [Arguments(10_000)]
    public async Task JsonWithParallelProcessingBenchmark(int recordCount)
    {
        _ctx.Parameters["recordCount"] = recordCount;

        await _runner.RunAsync<JsonParallelProcessingPipeline>(_ctx);
    }

    [Benchmark]
    [Arguments(1_000)]
    [Arguments(10_000)]
    public async Task MixedCsvJsonProcessingBenchmark(int recordCount)
    {
        _ctx.Parameters["recordCount"] = recordCount;

        await _runner.RunAsync<MixedProcessingPipeline>(_ctx);
    }

    // Data models
    public record ProcessedCsvRecord(
        int Id,
        string Name,
        decimal Price,
        DateTime Timestamp,
        bool IsValid,
        string Category,
        decimal TaxAmount,
        DateTime ProcessedAt
    );

    public record ProcessedJsonRecord(
        string UserId,
        string EventType,
        DateTime Timestamp,
        bool IsValid,
        Dictionary<string, object> Properties,
        DateTime ProcessedAt
    );

    public record MixedProcessingResult(
        int Id,
        string SourceType,
        DateTime Timestamp,
        bool IsValid,
        DateTime ProcessedAt
    );

    // Pipeline definitions
    private sealed class CsvProcessingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<CsvDataSource, CsvRecord>("src");
            var parser = b.AddTransform<CsvParser, CsvRecord, CsvRecord>("parser");
            var validator = b.AddTransform<CsvValidator, CsvRecord, CsvRecord>("validator");
            var enricher = b.AddTransform<CsvEnricher, CsvRecord, ProcessedCsvRecord>("enricher");
            var sink = b.AddSink<CsvDataSink, ProcessedCsvRecord>("sink");

            b.Connect(src, parser).Connect(parser, validator).Connect(validator, enricher).Connect(enricher, sink);
        }
    }

    private sealed class JsonProcessingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<JsonDataSource, JsonRecord>("src");
            var parser = b.AddTransform<JsonParser, JsonRecord, JsonRecord>("parser");
            var validator = b.AddTransform<JsonValidator, JsonRecord, JsonRecord>("validator");
            var transformer = b.AddTransform<JsonTransformer, JsonRecord, ProcessedJsonRecord>("transformer");
            var sink = b.AddSink<JsonDataSink, ProcessedJsonRecord>("sink");

            b.Connect(src, parser).Connect(parser, validator).Connect(validator, transformer).Connect(transformer, sink);
        }
    }

    private sealed class CsvParallelProcessingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<CsvDataSource, CsvRecord>("src");
            var parser = b.AddTransform<CsvParser, CsvRecord, CsvRecord>("parser");
            var validator = b.AddTransform<CsvValidator, CsvRecord, CsvRecord>("validator");
            var enricher = b.AddTransform<CsvEnricher, CsvRecord, ProcessedCsvRecord>("enricher");
            var sink = b.AddSink<CsvDataSink, ProcessedCsvRecord>("sink");

            b.Connect(src, parser).Connect(parser, validator).Connect(validator, enricher).Connect(enricher, sink);
        }
    }

    private sealed class JsonParallelProcessingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<JsonDataSource, JsonRecord>("src");
            var parser = b.AddTransform<JsonParser, JsonRecord, JsonRecord>("parser");
            var validator = b.AddTransform<JsonValidator, JsonRecord, JsonRecord>("validator");
            var transformer = b.AddTransform<JsonTransformer, JsonRecord, ProcessedJsonRecord>("transformer");
            var sink = b.AddSink<JsonDataSink, ProcessedJsonRecord>("sink");

            b.Connect(src, parser).Connect(parser, validator).Connect(validator, transformer).Connect(transformer, sink);
        }
    }

    private sealed class MixedProcessingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var src = b.AddSource<MixedDataSource, object>("src");
            var parser = b.AddTransform<MixedDataParser, object, object>("parser");
            var validator = b.AddTransform<MixedDataValidator, object, object>("validator");
            var enricher = b.AddTransform<MixedDataEnricher, object, MixedProcessingResult>("enricher");
            var sink = b.AddSink<MixedDataSink, MixedProcessingResult>("sink");

            b.Connect(src, parser).Connect(parser, validator).Connect(validator, enricher).Connect(enricher, sink);
        }
    }

    // Pipeline components for CSV processing
    private sealed class CsvDataSource : SourceNode<CsvRecord>
    {
        public override IDataPipe<CsvRecord> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
        {
            var count = context.Parameters.TryGetValue("recordCount", out var v)
                ? Convert.ToInt32(v)
                : 0;

            return new StreamingDataPipe<CsvRecord>(
                GenerateCsvRecords(count, cancellationToken),
                "csvSource");
        }
    }

    private sealed class CsvParser : TransformNode<CsvRecord, CsvRecord>
    {
        public override async Task<CsvRecord> ExecuteAsync(CsvRecord item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Simulate CSV parsing overhead
            await Task.Delay(2, cancellationToken);
            return item;
        }
    }

    private sealed class CsvValidator : TransformNode<CsvRecord, CsvRecord>
    {
        public override async Task<CsvRecord> ExecuteAsync(CsvRecord item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Simulate validation logic
            await Task.Delay(1, cancellationToken);
            return item with { Salary = Math.Max(0, item.Salary) }; // Ensure non-negative salary
        }
    }

    private sealed class CsvEnricher : TransformNode<CsvRecord, ProcessedCsvRecord>
    {
        public override async Task<ProcessedCsvRecord> ExecuteAsync(CsvRecord item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Simulate data enrichment
            await Task.Delay(3, cancellationToken);
            var taxRate = 0.08m; // 8% tax
            var taxAmount = item.Salary * taxRate;

            return new ProcessedCsvRecord(
                item.Id,
                $"{item.FirstName} {item.LastName}",
                item.Salary,
                item.JoinDate,
                item.Age >= 18 && item.Age <= 65,
                item.City,
                taxAmount,
                DateTime.UtcNow
            );
        }
    }

    private sealed class CsvDataSink : SinkNode<ProcessedCsvRecord>
    {
        public override async Task ExecuteAsync(IDataPipe<ProcessedCsvRecord> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                // Simulate processing/storage
                await Task.Delay(1, cancellationToken);
            }
        }
    }

    // Pipeline components for JSON processing
    private sealed class JsonDataSource : SourceNode<JsonRecord>
    {
        public override IDataPipe<JsonRecord> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
        {
            var count = context.Parameters.TryGetValue("recordCount", out var v)
                ? Convert.ToInt32(v)
                : 0;

            return new StreamingDataPipe<JsonRecord>(
                GenerateJsonRecords(count, cancellationToken),
                "jsonSource");
        }
    }

    private sealed class JsonParser : TransformNode<JsonRecord, JsonRecord>
    {
        public override async Task<JsonRecord> ExecuteAsync(JsonRecord item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Simulate JSON parsing overhead
            await Task.Delay(2, cancellationToken);
            return item;
        }
    }

    private sealed class JsonValidator : TransformNode<JsonRecord, JsonRecord>
    {
        public override async Task<JsonRecord> ExecuteAsync(JsonRecord item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Simulate validation logic
            await Task.Delay(1, cancellationToken);
            return item with { Status = item.Status.ToUpperInvariant() };
        }
    }

    private sealed class JsonTransformer : TransformNode<JsonRecord, ProcessedJsonRecord>
    {
        public override async Task<ProcessedJsonRecord> ExecuteAsync(JsonRecord item, PipelineContext context, CancellationToken cancellationToken)
        {
            // Simulate data transformation
            await Task.Delay(3, cancellationToken);

            var properties = new Dictionary<string, object>
            {
                ["originalEvent"] = item.Status,
                ["timestamp"] = item.Metadata.TryGetValue("created", out var created)
                    ? created
                    : DateTime.UtcNow,
                ["processed"] = true,
            };

            return new ProcessedJsonRecord(
                item.ProductId,
                item.Status,
                item.Metadata.TryGetValue("updated", out var updated) && updated is DateTime dt
                    ? dt
                    : DateTime.UtcNow,
                item.InStock > 0,
                properties,
                DateTime.UtcNow
            );
        }
    }

    private sealed class JsonDataSink : SinkNode<ProcessedJsonRecord>
    {
        public override async Task ExecuteAsync(IDataPipe<ProcessedJsonRecord> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                // Simulate processing/storage
                await Task.Delay(1, cancellationToken);
            }
        }
    }

    // Pipeline components for mixed processing
    private sealed class MixedDataSource : SourceNode<object>
    {
        public override IDataPipe<object> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
        {
            var count = context.Parameters.TryGetValue("recordCount", out var v)
                ? Convert.ToInt32(v)
                : 0;

            return new StreamingDataPipe<object>(GenerateItems(count, cancellationToken), "mixedSource");
        }

        private static async IAsyncEnumerable<object> GenerateItems(int count, [EnumeratorCancellation] CancellationToken ct)
        {
            await Task.Yield();

            var useCsv = true;

            await foreach (var csvItem in GenerateCsvRecords(count, ct))
            {
                if (useCsv)
                    yield return csvItem;

                useCsv = !useCsv;
            }

            await foreach (var jsonItem in GenerateJsonRecords(count, ct))
            {
                if (!useCsv)
                    yield return jsonItem;

                useCsv = !useCsv;
            }
        }
    }

    private sealed class MixedDataParser : TransformNode<object, object>
    {
        public override async Task<object> ExecuteAsync(object item, PipelineContext context, CancellationToken cancellationToken)
        {
            await Task.Delay(2, cancellationToken);
            return item;
        }
    }

    private sealed class MixedDataValidator : TransformNode<object, object>
    {
        public override async Task<object> ExecuteAsync(object item, PipelineContext context, CancellationToken cancellationToken)
        {
            await Task.Delay(1, cancellationToken);
            return item;
        }
    }

    private sealed class MixedDataEnricher : TransformNode<object, MixedProcessingResult>
    {
        public override async Task<MixedProcessingResult> ExecuteAsync(object item, PipelineContext context, CancellationToken cancellationToken)
        {
            await Task.Delay(3, cancellationToken);

            var sourceType = item switch
            {
                CsvRecord csv => "CSV",
                JsonRecord json => "JSON",
                _ => "Unknown",
            };

            var id = item switch
            {
                CsvRecord csv => csv.Id,
                JsonRecord json => int.Parse(json.ProductId.Replace("PROD-", "")),
                _ => 0,
            };

            return new MixedProcessingResult(
                id,
                sourceType,
                DateTime.UtcNow,
                true,
                DateTime.UtcNow
            );
        }
    }

    private sealed class MixedDataSink : SinkNode<MixedProcessingResult>
    {
        public override async Task ExecuteAsync(IDataPipe<MixedProcessingResult> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                await Task.Delay(1, cancellationToken);
            }
        }
    }
}
