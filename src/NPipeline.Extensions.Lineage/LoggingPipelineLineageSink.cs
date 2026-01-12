using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace NPipeline.Lineage;

/// <summary>
///     Sink that logs pipeline lineage reports to an ILogger.
/// </summary>
public sealed class LoggingPipelineLineageSink : IPipelineLineageSink
{
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly ILogger _logger;

    /// <summary>
    ///     Initializes a new instance of the <see cref="LoggingPipelineLineageSink" /> class.
    /// </summary>
    /// <param name="logger">The logger to write lineage reports to.</param>
    /// <param name="jsonOptions">Optional JSON serialization options.</param>
    public LoggingPipelineLineageSink(ILogger<LoggingPipelineLineageSink>? logger = null, JsonSerializerOptions? jsonOptions = null)
    {
        _logger = logger ?? NullLogger<LoggingPipelineLineageSink>.Instance;

        _jsonOptions = jsonOptions ?? new JsonSerializerOptions
        {
            WriteIndented = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        };
    }

    /// <summary>
    ///     Asynchronously records a pipeline lineage report.
    /// </summary>
    /// <param name="report">The pipeline lineage report.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    public Task RecordAsync(PipelineLineageReport report, CancellationToken cancellationToken)
    {
        if (report == null)
            return Task.CompletedTask;

        try
        {
            var json = JsonSerializer.Serialize(report, _jsonOptions);

            using (_logger.BeginScope(new Dictionary<string, object?>
                   {
                       ["Pipeline"] = report.Pipeline,
                       ["RunId"] = report.RunId,
                       ["NodeCount"] = report.Nodes.Count,
                       ["EdgeCount"] = report.Edges.Count,
                   }))
            {
                _logger.LogInformation(
                    "Pipeline lineage report for {Pipeline} (RunId: {RunId}): {LineageReport}",
                    report.Pipeline,
                    report.RunId,
                    json);
            }
        }
        catch
        {
            // Never throw exceptions from logging sink
            // Log a simplified message if JSON serialization fails
            _logger.LogInformation(
                "Pipeline lineage report for {Pipeline} (RunId: {RunId}) with {NodeCount} nodes and {EdgeCount} edges",
                report.Pipeline,
                report.RunId,
                report.Nodes.Count,
                report.Edges.Count);
        }

        return Task.CompletedTask;
    }
}
