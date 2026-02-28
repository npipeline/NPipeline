using System.ComponentModel.DataAnnotations.Schema;
using NPipeline.Connectors.DuckDB.Attributes;

namespace NPipeline.Connectors.DuckDB.Tests;

/// <summary>
///     Shared model classes for tests.
/// </summary>
public class TestRecord
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public double Value { get; set; }
}

public class NullableTestRecord
{
    public int Id { get; set; }
    public string? Name { get; set; }
    public double? OptionalValue { get; set; }
    public DateTime? CreatedAt { get; set; }
}

[Table("custom_table")]
public class CustomColumnRecord
{
    [DuckDBColumn("record_id", PrimaryKey = true)]
    public int RecordId { get; set; }

    [DuckDBColumn("record_name")]
    public string RecordName { get; set; } = string.Empty;

    [DuckDBColumn(Ignore = true)]
    public string? Ignored { get; set; }
}

public class SnakeCaseRecord
{
    public int user_id { get; set; }
    public string first_name { get; set; } = string.Empty;
    public string last_name { get; set; } = string.Empty;
}

public class AllTypesRecord
{
    public bool BoolValue { get; set; }
    public byte ByteValue { get; set; }
    public short ShortValue { get; set; }
    public int IntValue { get; set; }
    public long LongValue { get; set; }
    public float FloatValue { get; set; }
    public double DoubleValue { get; set; }
    public decimal DecimalValue { get; set; }
    public string StringValue { get; set; } = string.Empty;
    public DateTime DateTimeValue { get; set; }
}

public class GuidRecord
{
    public int Id { get; set; }
    public Guid TraceId { get; set; }
}

public class EnumRecord
{
    public int Id { get; set; }
    public TestStatus Status { get; set; }
}

public enum TestStatus
{
    Active,
    Inactive,
    Pending,
}

public record PositionalRecord(int Id, string Name, double Value);

public class LargeRecord
{
    public int Id { get; set; }
    public string Data { get; set; } = string.Empty;
    public double NumericValue { get; set; }
    public DateTime Timestamp { get; set; }
}
