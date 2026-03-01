using BenchmarkDotNet.Running;

namespace NPipeline.Connectors.Aws.Redshift.Benchmarks;

public static class Program
{
    public static void Main(string[] args)
    {
        var summary = BenchmarkRunner.Run<RedshiftWriteStrategyBenchmarks>();
        BenchmarkRunner.Run<RedshiftMapperBenchmarks>();
    }
}
