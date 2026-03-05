using System.Reflection;
using Microsoft.Extensions.Hosting;
using NPipeline.Extensions.DependencyInjection;
using Sample_HttpConnector.Pipelines;

namespace Sample_HttpConnector;

public sealed class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: HTTP Connector ===");
        Console.WriteLine();
        Console.WriteLine(GithubToSlackPipeline.GetDescription());
        Console.WriteLine();

        try
        {
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((_, services) =>
                {
                    services.AddNPipeline(Assembly.GetExecutingAssembly());
                    GithubToSlackPipeline.RegisterServices(services);
                })
                .Build();

            Console.WriteLine("Starting pipeline execution...");
            Console.WriteLine();

            await host.Services.RunPipelineAsync<GithubToSlackPipeline>();

            Console.WriteLine();
            Console.WriteLine("Pipeline completed successfully.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Pipeline failed: {ex.Message}");
            Console.WriteLine();
            Console.WriteLine(ex.ToString());
            Environment.ExitCode = 1;
        }
    }
}
