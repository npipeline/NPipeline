using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Lineage;
using NPipeline.Lineage.DependencyInjection;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Lineage.Tests;

/// <summary>
///     Covers the wiring that decides whether lineage is actually reported for a run: pipeline-level reports
///     (which describe the graph and need no per-item tracking), the item-level collector, and the warning
///     emitted when an item-level sink would otherwise be silently dropped.
/// </summary>
public sealed class PipelineLineageReportingTests
{
    [Fact]
    public async Task PipelineLineageSink_WithoutItemLevelLineage_ShouldStillReceiveReport()
    {
        var sink = new RecordingPipelineLineageSink();

        // The sink is attached through the builder, so this is the plain "register a pipeline lineage sink"
        // path. The pipeline deliberately does not call EnableItemLevelLineage: that used to suppress the
        // report entirely.
        _ = await RunPipelineAsync<ReportOnlyPipeline>(
            new ContextSeed().With(PipelineSinkKey, sink),
            services => services.AddNPipelineLineage());

        sink.Reports.Should().HaveCount(1);
        var report = sink.Reports[0];
        report.Pipeline.Should().Be(nameof(ReportOnlyPipeline));
        report.Nodes.Select(n => n.Id).Should().BeEquivalentTo(["source", "sink"]);
        report.Edges.Should().ContainSingle(e => e.From == "source" && e.To == "sink");
    }

    [Fact]
    public async Task AddNPipelineLineage_WithoutExplicitSink_ShouldReportViaDefaultProvider()
    {
        var seed = new ContextSeed();

        // The provider-supplied default sink is only registered by AddNPipelineLineage, so the run resolving one
        // proves the provider path is no longer gated behind item-level lineage.
        await using var provider = await BuildProviderAndRunAsync<ReportOnlyPipeline>(seed, services => services.AddNPipelineLineage());

        provider.GetService<IPipelineLineageSinkProvider>().Should().NotBeNull();
        seed.Context!.Lineage.PipelineLineageSink.Should().NotBeNull();
    }

    [Fact]
    public async Task ItemLevelLineage_ShouldPopulateCollectorOnContext()
    {
        var context = await RunPipelineAsync<ItemLevelLineagePipeline>(new ContextSeed(), services => services.AddNPipelineLineage());

        context.Lineage.LineageCollector.Should().NotBeNull();

        var records = context.Lineage.LineageCollector!.GetAllRecords();
        records.Should().NotBeEmpty();
        records.Select(r => r.CorrelationId).Distinct().Should().HaveCount(3);
    }

    [Fact]
    public async Task ItemLevelLineage_ShouldFeedCollectorAndConfiguredSinkAlike()
    {
        var itemSink = new RecordingLineageSink();
        var seed = new ContextSeed().With(ItemSinkKey, itemSink);

        var context = await RunPipelineAsync<ItemLevelLineageWithSinkPipeline>(seed, services => services.AddNPipelineLineage());

        itemSink.Records.Should().NotBeEmpty();
        context.Lineage.LineageCollector.Should().NotBeNull();

        context.Lineage.LineageCollector!.GetAllRecords()
            .Select(r => r.CorrelationId)
            .Distinct()
            .Should()
            .BeEquivalentTo(itemSink.Records.Select(r => r.CorrelationId).Distinct());
    }

    [Fact]
    public async Task WithoutItemLevelLineage_ShouldNotResolveCollector()
    {
        var context = await RunPipelineAsync<ReportOnlyPipeline>(new ContextSeed(), services => services.AddNPipelineLineage());

        context.Lineage.LineageCollector.Should().BeNull();
    }

    [Fact]
    public async Task ItemLevelLineageSinkConfigured_WithItemLevelLineageOff_ShouldWarn()
    {
        var logger = new CapturingLoggerProvider();

        _ = await RunPipelineAsync<IgnoredItemSinkPipeline>(
            new ContextSeed(),
            services =>
            {
                services.AddNPipelineLineage();
                services.AddLogging(b => b.AddProvider(logger).SetMinimumLevel(LogLevel.Debug));
            });

        logger.Warnings.Should().ContainSingle(w =>
            w.Contains(nameof(RecordingLineageSink), StringComparison.Ordinal) &&
            w.Contains("EnableItemLevelLineage", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ItemLevelLineageSinkConfigured_WithItemLevelLineageOn_ShouldNotWarn()
    {
        var logger = new CapturingLoggerProvider();

        _ = await RunPipelineAsync<ItemLevelLineageWithSinkPipeline>(
            new ContextSeed().With(ItemSinkKey, new RecordingLineageSink()),
            services =>
            {
                services.AddNPipelineLineage();
                services.AddLogging(b => b.AddProvider(logger).SetMinimumLevel(LogLevel.Debug));
            });

        logger.Warnings.Should().NotContain(w => w.Contains("EnableItemLevelLineage", StringComparison.Ordinal));
    }

    private const string ItemSinkKey = "testing.item.lineage.sink";
    private const string PipelineSinkKey = "testing.pipeline.lineage.sink";

    private static async Task<PipelineContext> RunPipelineAsync<TPipeline>(ContextSeed seed, Action<IServiceCollection> configure)
        where TPipeline : IPipelineDefinition, new()
    {
        await using var provider = await BuildProviderAndRunAsync<TPipeline>(seed, configure);
        return seed.Context!;
    }

    /// <summary>
    ///     Builds a context the way the DI entry point does, so lineage components resolve from the container
    ///     rather than from the non-DI defaults a bare <see cref="PipelineContext" /> falls back to.
    /// </summary>
    private static async Task<ServiceProvider> BuildProviderAndRunAsync<TPipeline>(ContextSeed seed, Action<IServiceCollection> configure)
        where TPipeline : IPipelineDefinition, new()
    {
        var services = new ServiceCollection();
        services.AddNPipeline(typeof(PipelineLineageReportingTests).Assembly);
        configure(services);

        var provider = services.BuildServiceProvider();

        var config = new PipelineContextConfiguration(
            ErrorHandlerFactory: provider.GetRequiredService<IErrorHandlerFactory>(),
            LineageFactory: provider.GetRequiredService<ILineageFactory>(),
            ObservabilityFactory: provider.GetRequiredService<IObservabilityFactory>(),
            LoggerFactory: provider.GetService<ILoggerFactory>());

        var context = new PipelineContext(config);
        seed.Apply(context);

        var runner = provider.GetRequiredService<IPipelineRunner>();
        await runner.RunAsync<TPipeline>(context);

        return provider;
    }

    /// <summary>
    ///     Carries items that must be on the context before the run, and hands the context back afterwards so
    ///     tests can assert on what the run wired onto it.
    /// </summary>
    private sealed class ContextSeed
    {
        private readonly Dictionary<string, object> _items = [];

        public PipelineContext? Context { get; private set; }

        public ContextSeed With(string key, object value)
        {
            _items[key] = value;
            return this;
        }

        public void Apply(PipelineContext context)
        {
            foreach (var (key, value) in _items)
                context.Items[key] = value;

            Context = context;
        }
    }

    /// <summary>
    ///     Two-node pipeline that never enables item-level lineage, and attaches a pipeline-level sink only when
    ///     the test seeded one.
    /// </summary>
    public sealed class ReportOnlyPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            if (context.Items.TryGetValue(PipelineSinkKey, out var seeded) && seeded is IPipelineLineageSink pipelineSink)
                _ = builder.AddPipelineLineageSink(pipelineSink);

            var source = builder.AddSource<NumbersSourceNode, int>("source");
            var sink = builder.AddSink<CollectingSinkNode, int>("sink");
            _ = builder.Connect(source, sink);
        }
    }

    /// <summary>Same shape, with item-level lineage switched on.</summary>
    public sealed class ItemLevelLineagePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            _ = builder.EnableItemLevelLineage();
            var source = builder.AddSource<NumbersSourceNode, int>("source");
            var sink = builder.AddSink<CollectingSinkNode, int>("sink");
            _ = builder.Connect(source, sink);
        }
    }

    /// <summary>Item-level lineage on, with an explicitly configured item-level sink.</summary>
    public sealed class ItemLevelLineageWithSinkPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            _ = builder.EnableItemLevelLineage();
            _ = builder.AddLineageSink((ILineageSink)context.Items[ItemSinkKey]!);
            var source = builder.AddSource<NumbersSourceNode, int>("source");
            var sink = builder.AddSink<CollectingSinkNode, int>("sink");
            _ = builder.Connect(source, sink);
        }
    }

    /// <summary>An item-level sink configured without item-level lineage: the case that warrants a warning.</summary>
    public sealed class IgnoredItemSinkPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            _ = builder.AddLineageSink<RecordingLineageSink>();
            var source = builder.AddSource<NumbersSourceNode, int>("source");
            var sink = builder.AddSink<CollectingSinkNode, int>("sink");
            _ = builder.Connect(source, sink);
        }
    }

    public sealed class NumbersSourceNode : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
            => new InMemoryDataStream<int>([1, 2, 3], "numbers");
    }

    public sealed class CollectingSinkNode : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                // Drain: the assertions are about lineage wiring, not about the payload.
            }
        }
    }

    public sealed class RecordingLineageSink : ILineageSink
    {
        private readonly List<LineageRecord> _records = [];
        private readonly object _sync = new();

        public IReadOnlyList<LineageRecord> Records
        {
            get
            {
                lock (_sync)
                {
                    return _records.ToList();
                }
            }
        }

        public Task RecordAsync(LineageRecord record, CancellationToken cancellationToken)
        {
            lock (_sync)
            {
                _records.Add(record);
            }

            return Task.CompletedTask;
        }
    }

    private sealed class RecordingPipelineLineageSink : IPipelineLineageSink
    {
        private readonly List<PipelineLineageReport> _reports = [];
        private readonly object _sync = new();

        public IReadOnlyList<PipelineLineageReport> Reports
        {
            get
            {
                lock (_sync)
                {
                    return _reports.ToList();
                }
            }
        }

        public Task RecordAsync(PipelineLineageReport report, CancellationToken cancellationToken)
        {
            lock (_sync)
            {
                _reports.Add(report);
            }

            return Task.CompletedTask;
        }
    }

    private sealed class CapturingLoggerProvider : ILoggerProvider
    {
        private readonly List<string> _warnings = [];
        private readonly object _sync = new();

        public IReadOnlyList<string> Warnings
        {
            get
            {
                lock (_sync)
                {
                    return _warnings.ToList();
                }
            }
        }

        public ILogger CreateLogger(string categoryName) => new CapturingLogger(this);

        public void Dispose()
        {
        }

        private void Add(string message)
        {
            lock (_sync)
            {
                _warnings.Add(message);
            }
        }

        private sealed class CapturingLogger(CapturingLoggerProvider owner) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                if (logLevel >= LogLevel.Warning)
                    owner.Add(formatter(state, exception));
            }
        }
    }
}
