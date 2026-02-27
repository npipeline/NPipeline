using NPipeline.Connectors.Parquet;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Models;
using Sample_ParquetConnector.Nodes;

namespace Sample_ParquetConnector;

/// <summary>
///     Demonstrates writing to and reading from a local Parquet file.
///     Pipeline: SalesDataSourceNode → ParquetSinkNode
/// </summary>
public sealed class ParquetConnectorPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(new SalesDataSourceNode(500), "sales-source");
        var sink = builder.AddSink(new ParquetSinkNode<SalesRecord>(StorageUri.FromFilePath(GetOutputPath())), "parquet-sink");

        builder.Connect(source, sink);
    }

    public static string GetOutputPath()
    {
        var dir = Path.Combine(Directory.GetCurrentDirectory(), "output");
        Directory.CreateDirectory(dir);
        return Path.Combine(dir, "sales.parquet");
    }
}
