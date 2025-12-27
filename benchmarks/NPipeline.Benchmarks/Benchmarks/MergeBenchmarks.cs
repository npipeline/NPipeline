using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Execution.Strategies;

namespace NPipeline.Benchmarks.Benchmarks;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class MergeBenchmarks
{
    private List<IDataPipe> _pipes = [];

    [Params(2, 4)]
    public int Producers { get; set; }

    [Params(1_000, 10_000)]
    public int ItemsPerProducer { get; set; }

    // 0 == null capacity (unbounded)
    [Params(0, 1024)]
    public int Capacity { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _pipes = new List<IDataPipe>(Producers);

        for (var p = 0; p < Producers; p++)
        {
            var stream = Produce(ItemsPerProducer, p);
            _pipes.Add(new StreamingDataPipe<int>(stream, $"P{p}"));
        }
    }

    [Benchmark(Baseline = true, Description = "Concatenate (baseline)")]
    public async Task<long> Concatenate_Baseline()
    {
        var merged = MergeStrategies.Concatenate<int>(_pipes, CancellationToken.None);
        return await Consume(merged);
    }

    [Benchmark(Description = "Interleave (unbounded)")]
    public async Task<long> Interleave_Unbounded()
    {
        var merged = MergeStrategies.Interleave<int>(_pipes, CancellationToken.None);
        return await Consume(merged);
    }

    [Benchmark(Description = "Interleave (bounded)")]
    public async Task<long> Interleave_Bounded()
    {
        int? cap = Capacity == 0
            ? null
            : Capacity;

        var merged = MergeStrategies.InterleaveBounded<int>(_pipes, cap, CancellationToken.None);
        return await Consume(merged);
    }

    private static async Task<long> Consume(IAsyncEnumerable<int> source, CancellationToken ct = default)
    {
        long sum = 0;

        await foreach (var item in source.WithCancellation(ct))
        {
            sum += item;
        }

        return sum;
    }

    private static async IAsyncEnumerable<int> Produce(int count, int seed,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        // Small yield to simulate async and avoid tight CPU-bound loops
        await Task.Yield();

        // Simple deterministic stream
        for (var i = 0; i < count; i++)
        {
            ct.ThrowIfCancellationRequested();
            yield return i + seed;
        }
    }
}
