using NPipeline.Connectors.DataLake;
using NPipeline.Connectors.DataLake.Partitioning;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Models;
using Sample_DataLakeConnector.Nodes;

namespace Sample_DataLakeConnector;

/// <summary>
///     Demonstrates writing partitioned data to a Data Lake table.
///     Pipeline: SalesDataSourceNode → DataLakePartitionedSinkNode (partitioned by EventDate, Region)
/// </summary>
public sealed class DataLakeConnectorPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var (tableUri, _) = GetTableLocation();

        var partitionSpec = PartitionSpec<SalesRecord>
            .By(x => x.EventDate)
            .ThenBy(x => x.Region);

        var source = builder.AddSource(new SalesDataSourceNode(), "sales-source");
        var sink = builder.AddSink(new DataLakePartitionedSinkNode<SalesRecord>(tableUri, partitionSpec), "datalake-sink");

        builder.Connect(source, sink);
    }

    public static (StorageUri tableUri, string tablePath) GetTableLocation()
    {
        var tablePath = Path.Combine(Directory.GetCurrentDirectory(), "output", "sales_table");
        Directory.CreateDirectory(tablePath);
        return (StorageUri.FromFilePath(tablePath), tablePath);
    }
}
