using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;

namespace NPipeline.Benchmarks;

public static class Program
{
    public static void Main(string[] args)
    {
        var config = ManualConfig.CreateEmpty()
            .WithOptions(ConfigOptions.DisableOptimizationsValidator);

        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
    }
}
