using NPipeline.Connectors.DuckDB.Attributes;

namespace Sample_DuckDBConnector;

/// <summary>
///     Represents a sensor reading for the DuckDB sample pipeline.
/// </summary>
public sealed class SensorReading
{
    [DuckDBColumn("id", PrimaryKey = true)]
    public int Id { get; set; }

    [DuckDBColumn("sensor_name")]
    public string SensorName { get; set; } = string.Empty;

    [DuckDBColumn("temperature")]
    public double Temperature { get; set; }

    [DuckDBColumn("humidity")]
    public double Humidity { get; set; }

    [DuckDBColumn("recorded_at")]
    public DateTime RecordedAt { get; set; }

    [DuckDBColumn("region")]
    public string Region { get; set; } = string.Empty;
}
