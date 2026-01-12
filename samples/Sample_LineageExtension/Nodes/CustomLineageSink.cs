using NPipeline.Lineage;
using System.Text.Json;

namespace Sample_LineageExtension.Nodes
{
    /// <summary>
    ///     Custom pipeline lineage sink that collects and exports lineage information.
    ///     Demonstrates how to implement a custom IPipelineLineageSink for specialized lineage processing.
    /// </summary>
    public class CustomLineageSink : IPipelineLineageSink
    {
        private readonly List<PipelineLineageReport> _reports = [];
        private readonly string _outputPath;
        private readonly JsonSerializerOptions _jsonOptions;

        /// <summary>
        ///     Initializes a new instance of <see cref="CustomLineageSink" /> class.
        /// </summary>
        /// <param name="outputPath">Optional file path to export lineage reports. If null, reports are kept in memory.</param>
        /// <param name="jsonOptions">Optional JSON serialization options.</param>
        public CustomLineageSink(string? outputPath = null, JsonSerializerOptions? jsonOptions = null)
        {
            _outputPath = outputPath ?? "lineage-reports.json";
            _jsonOptions = jsonOptions ?? new JsonSerializerOptions
            {
                WriteIndented = true,
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            };
        }

        /// <summary>
        ///     Gets all collected lineage reports.
        /// </summary>
        public IReadOnlyList<PipelineLineageReport> Reports => _reports.AsReadOnly();

        /// <summary>
        ///     Asynchronously records a pipeline lineage report.
        /// </summary>
        /// <param name="report">The pipeline lineage report.</param>
        /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
        /// <returns>A <see cref="Task" /> representing asynchronous operation.</returns>
        public Task RecordAsync(PipelineLineageReport report, CancellationToken cancellationToken)
        {
            if (report == null)
            {
                return Task.CompletedTask;
            }

            // Store the report
            _reports.Add(report);

            // Display summary information
            Console.WriteLine();
            Console.WriteLine("=== Pipeline Lineage Report ===");
            Console.WriteLine($"Pipeline: {report.Pipeline}");
            Console.WriteLine($"Run ID: {report.RunId}");
            Console.WriteLine($"Nodes: {report.Nodes.Count}");
            Console.WriteLine($"Edges: {report.Edges.Count}");
            Console.WriteLine();

            // Display node information
            Console.WriteLine("Nodes:");
            foreach (var node in report.Nodes)
            {
                Console.WriteLine($"  - {node.Id} ({node.Type})");
            }
            Console.WriteLine();

            // Display edge information
            Console.WriteLine("Edges:");
            foreach (var edge in report.Edges)
            {
                Console.WriteLine($"  - {edge.From} -> {edge.To}");
            }
            Console.WriteLine();

            // Export to file
            ExportToFile(report, cancellationToken).GetAwaiter().GetResult();

            return Task.CompletedTask;
        }

        /// <summary>
        ///     Exports lineage report to a JSON file.
        /// </summary>
        private async Task ExportToFile(PipelineLineageReport report, CancellationToken cancellationToken)
        {
            try
            {
                var json = JsonSerializer.Serialize(report, _jsonOptions);
                var fileName = $"{_outputPath.Replace(".json", string.Empty, StringComparison.InvariantCulture)}_{DateTime.UtcNow:yyyyMMdd_HHmmss}.json";

                await File.WriteAllTextAsync(fileName, json, cancellationToken);

                Console.WriteLine($"[CustomLineageSink] Lineage report exported to: {fileName}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[CustomLineageSink] Error exporting lineage report: {ex.Message}");
            }
        }

        /// <summary>
        ///     Clears all collected reports.
        /// </summary>
        public void Clear()
        {
            _reports.Clear();
        }

        /// <summary>
        ///     Gets a summary of all collected reports.
        /// </summary>
        public string GetSummary()
        {
            if (_reports.Count == 0)
            {
                return "No lineage reports collected.";
            }

            var summary = $"Lineage Reports Summary:\n";
            summary += $"Total Reports: {_reports.Count}\n";
            summary += $"Pipelines: {_reports.Select(r => r.Pipeline).Distinct().Count()}\n";
            summary += $"Total Nodes: {_reports.Sum(r => r.Nodes.Count)}\n";
            summary += $"Total Edges: {_reports.Sum(r => r.Edges.Count)}\n";

            return summary;
        }
    }
}