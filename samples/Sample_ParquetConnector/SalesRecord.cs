using NPipeline.Connectors.Parquet.Attributes;

namespace Sample_ParquetConnector;

public sealed class SalesRecord
{
    public int Id { get; set; }
    public string Product { get; set; } = string.Empty;

    [ParquetDecimal(18, 2)]
    public decimal Amount { get; set; }

    public DateTime TransactionDate { get; set; }
    public string Region { get; set; } = string.Empty;
}
