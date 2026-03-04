namespace NPipeline.Connectors.MySql.Configuration;

/// <summary>
///     Specifies the CDC (Change Data Capture) position-tracking mode.
/// </summary>
public enum CdcMode
{
    /// <summary>
    ///     Track the MySQL binlog file name and byte offset.
    ///     Conservative and explicit; requires the same server instance to resume correctly.
    /// </summary>
    BinlogFile,

    /// <summary>
    ///     Track the GTID (Global Transaction ID) set.
    ///     Server-agnostic; supports resumption after primary failover.
    /// </summary>
    Gtid,
}
