using NPipeline.Connectors.Parquet.Attributes;

namespace Sample_DataLakeConnector;

public sealed class SalesRecord
{
    public int Id { get; set; }
    public string Product { get; set; } = string.Empty;

    [ParquetDecimal(18, 2)]
    public decimal Amount { get; set; }

    /// <summary>Partition column — controls the event_date= directory segment.</summary>
    public DateTime EventDate { get; set; }

    /// <summary>Partition column — controls the region= directory segment.</summary>
    public string Region { get; set; } = string.Empty;
}
