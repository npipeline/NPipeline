using System.Reflection;
using NPipeline.Extensions.DependencyInjection;
using Sample_HttpPost.Nodes;

namespace Sample_HttpPost;

/// <summary>
///     Entry point for the HTTP POST Webhook sample demonstrating the push-to-pull bridge pattern.
///     This sample shows how to receive HTTP POST requests and flow data through a pipeline
///     using a channel-based source node.
/// </summary>
public sealed class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: HTTP POST Webhook Processing ===");
        Console.WriteLine();

        try
        {
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, services) =>
                {
                    // Add NPipeline services with assembly scanning
                    _ = services.AddNPipeline(Assembly.GetExecutingAssembly());

                    // Register WebhookSource as a singleton so both the controller
                    // and the pipeline can access the same instance
                    _ = services.AddSingleton<WebhookSource>();

                    // Add ASP.NET Core controllers
                    _ = services.AddControllers();

                    // Add logging
                    _ = services.AddLogging(builder =>
                    {
                        builder.AddConsole();
                        builder.SetMinimumLevel(LogLevel.Information);
                    });
                })
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.Configure(app =>
                    {
                        var env = app.ApplicationServices.GetRequiredService<IWebHostEnvironment>();

                        if (env.IsDevelopment())
                            _ = app.UseDeveloperExceptionPage();

                        _ = app.UseRouting();
                        _ = app.UseEndpoints(endpoints => { endpoints.MapControllers(); });
                    });
                })
                .Build();

            Console.WriteLine("Registered NPipeline services and scanned assemblies for nodes.");
            Console.WriteLine();

            // Display pipeline description
            Console.WriteLine("Pipeline Description:");
            Console.WriteLine(WebhookPipeline.GetDescription());
            Console.WriteLine();

            // Start the pipeline in a background task
            var pipelineTask = Task.Run(async () =>
            {
                try
                {
                    Console.WriteLine("Starting pipeline execution in background...");
                    Console.WriteLine();

                    await host.Services.RunPipelineAsync<WebhookPipeline>();

                    Console.WriteLine();
                    Console.WriteLine("Pipeline execution completed!");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Pipeline execution error: {ex.Message}");
                    Console.WriteLine();
                    Console.WriteLine("Full error details:");
                    Console.WriteLine(ex.ToString());
                }
            });

            // Start the web server
            Console.WriteLine("Starting web server...");
            Console.WriteLine("Webhook endpoint available at: http://localhost:5000/api/webhook");
            Console.WriteLine("Status endpoint available at: http://localhost:5000/api/webhook/status");
            Console.WriteLine();
            Console.WriteLine("Press Ctrl+C to stop the application.");
            Console.WriteLine();

            await host.RunAsync();

            // Wait for the pipeline to complete when the host is shutting down
            await pipelineTask;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Application error: {ex.Message}");
            Console.WriteLine();
            Console.WriteLine("Full error details:");
            Console.WriteLine(ex.ToString());
            Environment.ExitCode = 1;
        }
    }
}
