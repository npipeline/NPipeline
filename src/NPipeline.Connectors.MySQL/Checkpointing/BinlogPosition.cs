namespace NPipeline.Connectors.MySql.Checkpointing;

/// <summary>
///     Represents the binlog position in a MySQL binary log stream.
///     Used for CDC checkpointing when GTID mode is not enabled.
/// </summary>
public sealed record BinlogPosition
{
    /// <summary>Gets or sets the binlog file name (e.g., <c>mysql-bin.000001</c>).</summary>
    public string? FileName { get; init; }

    /// <summary>Gets or sets the byte offset within the file.</summary>
    public long Position { get; init; }

    /// <summary>
    ///     Gets or sets the GTID set string (e.g., <c>3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5</c>).
    ///     Used when the server runs in GTID mode.
    /// </summary>
    public string? GtidSet { get; init; }

    /// <summary>Gets or sets the total number of events processed from this position.</summary>
    public long? EventCount { get; init; }

    /// <summary>Gets or sets the timestamp of the most recent processed event.</summary>
    public DateTimeOffset? LastEventTimestamp { get; init; }

    /// <summary>Creates a <see cref="BinlogPosition" /> from file/offset values.</summary>
    public static BinlogPosition FromFileOffset(string fileName, long position)
    {
        return new BinlogPosition { FileName = fileName, Position = position };
    }

    /// <summary>Creates a <see cref="BinlogPosition" /> from a GTID set string.</summary>
    public static BinlogPosition FromGtid(string gtidSet)
    {
        return new BinlogPosition { GtidSet = gtidSet };
    }

    /// <summary>Returns a human-readable representation of the position.</summary>
    public override string ToString()
    {
        return GtidSet is not null
            ? $"GTID:{GtidSet}"
            : $"{FileName ?? "?"}@{Position}";
    }
}
