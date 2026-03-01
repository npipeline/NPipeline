using System.Reflection;
using Microsoft.Extensions.Hosting;
using NPipeline.Extensions.DependencyInjection;

namespace Sample_RedshiftConnector;

public static class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: AWS Redshift Connector ===");
        Console.WriteLine();

        var connectionString = Environment.GetEnvironmentVariable("NPIPELINE_REDSHIFT_CONNECTION_STRING")
                               ?? (args.Length > 0
                                   ? args[0]
                                   : null);

        var s3Bucket = Environment.GetEnvironmentVariable("NPIPELINE_REDSHIFT_S3_BUCKET")
                       ?? (args.Length > 1
                           ? args[1]
                           : null);

        var iamRole = Environment.GetEnvironmentVariable("NPIPELINE_REDSHIFT_IAM_ROLE")
                      ?? (args.Length > 2
                          ? args[2]
                          : null);

        var awsRegion = Environment.GetEnvironmentVariable("AWS_DEFAULT_REGION") ?? "us-east-1";

        if (string.IsNullOrWhiteSpace(connectionString)
            || string.IsNullOrWhiteSpace(s3Bucket)
            || string.IsNullOrWhiteSpace(iamRole))
        {
            Console.Error.WriteLine("Required environment variables or arguments:");
            Console.Error.WriteLine("  NPIPELINE_REDSHIFT_CONNECTION_STRING");
            Console.Error.WriteLine("  NPIPELINE_REDSHIFT_S3_BUCKET");
            Console.Error.WriteLine("  NPIPELINE_REDSHIFT_IAM_ROLE");
            Console.Error.WriteLine();
            Console.Error.WriteLine("Example:");

            Console.Error.WriteLine(
                "  Host=my-cluster.us-east-1.redshift.amazonaws.com;Port=5439;Database=analytics;Username=etl;Password=secret;SSL Mode=Require");

            Environment.ExitCode = 1;
            return;
        }

        try
        {
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((_, services) => services.AddNPipeline(Assembly.GetExecutingAssembly()))
                .Build();

            Console.WriteLine("Running Redshift connector pipeline sample...");
            Console.WriteLine();

            var pipeline = new RedshiftConnectorPipeline(connectionString, s3Bucket, iamRole, awsRegion);
            await pipeline.ExecuteAsync(host.Services, CancellationToken.None);

            Console.WriteLine();
            Console.WriteLine("Pipeline completed successfully.");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Pipeline failed: {ex.Message}");
            Console.Error.WriteLine(ex.ToString());
            Environment.ExitCode = 1;
        }
    }
}
