namespace NPipeline.Connectors.Parquet.Attributes;

/// <summary>
///     Specifies the precision and scale for a <see cref="decimal" /> property when writing to Parquet files.
///     This attribute is required on decimal properties; otherwise, schema building will throw.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class ParquetDecimalAttribute(int precision, int scale) : Attribute
{
    /// <summary>
    ///     Gets the precision (total number of digits) for the decimal column.
    /// </summary>
    public int Precision { get; } = precision;

    /// <summary>
    ///     Gets the scale (number of digits after the decimal point) for the decimal column.
    /// </summary>
    public int Scale { get; } = scale;
}
