using NPipeline.Connectors.Aws.Redshift.Mapping;

namespace Sample_RedshiftConnector;

/// <summary>Raw event row read from Redshift.</summary>
public sealed class OrderEvent
{
    public long OrderId { get; set; }
    public string CustomerId { get; set; } = string.Empty;
    public string ProductSku { get; set; } = string.Empty;
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
    public DateTime OrderedAt { get; set; }
    public string Status { get; set; } = string.Empty;
}

/// <summary>Enriched summary row written to Redshift.</summary>
[RedshiftTable("order_summaries", Schema = "public")]
public sealed class OrderSummary
{
    public long OrderId { get; set; }
    public string CustomerId { get; set; } = string.Empty;
    public decimal Revenue { get; set; }
    public int ItemCount { get; set; }
    public DateTime OrderedAt { get; set; }
    public string Status { get; set; } = string.Empty;
    public DateTime ProcessedAt { get; set; }
}
