using System.Diagnostics;
using System.Diagnostics.Metrics;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Diagnostics;

/// <summary>
///     The metrics and traces every connector emits, under one name. Subscribe with <c>AddMeter(ConnectorDiagnostics.Name)</c>
///     and <c>AddSource(ConnectorDiagnostics.Name)</c> in OpenTelemetry.
/// </summary>
/// <remarks>
///     Metric tags are bounded: the connector (<c>csv</c>, <c>json</c>, …), the storage scheme and, for errors, the action
///     taken. Paths, query strings and record contents never appear in tags, so metric cardinality stays fixed and URI
///     secrets stay out of telemetry. Activities carry the path without its query string.
/// </remarks>
public static class ConnectorDiagnostics
{
    /// <summary>The meter and activity source name: <c>NPipeline.Connectors</c>.</summary>
    public const string Name = "NPipeline.Connectors";

    /// <summary>The connectors' activity source.</summary>
    public static ActivitySource ActivitySource { get; } = new(Name);

    /// <summary>The connectors' meter. Declared before the counters, which static initialisation creates from it in order.</summary>
    public static Meter Meter { get; } = new(Name);

    private static readonly Counter<long> RowsRead = Meter.CreateCounter<long>("npipeline.connector.rows_read", "{row}", "Records read by connector sources.");

    private static readonly Counter<long> RowsWritten = Meter.CreateCounter<long>("npipeline.connector.rows_written", "{row}", "Records written by connector sinks.");

    private static readonly Counter<long> BytesRead = Meter.CreateCounter<long>("npipeline.connector.bytes_read", "By", "Bytes read from storage, before decompression.");

    private static readonly Counter<long> BytesWritten = Meter.CreateCounter<long>("npipeline.connector.bytes_written", "By", "Bytes written to storage, after compression.");

    private static readonly Counter<long> FilesRead = Meter.CreateCounter<long>("npipeline.connector.files_read", "{file}", "Files read by connector sources.");

    private static readonly Counter<long> FilesWritten = Meter.CreateCounter<long>("npipeline.connector.files_written", "{file}", "Files written by connector sinks.");

    private static readonly Counter<long> RowErrors = Meter.CreateCounter<long>("npipeline.connector.row_errors", "{error}", "Records that failed to map, by the action taken.");

    private static readonly Counter<long> MessagesSettled = Meter.CreateCounter<long>("npipeline.connector.messages_settled", "{message}",
        "Messages settled by message-queue connectors, by outcome: acknowledged, requeued, rejected or dead_lettered.");

    /// <summary>Records records read by a source. Connectors that are not file-based (HTTP) call it directly.</summary>
    public static void RecordRowsRead(string connector, string scheme, long rows) => RowsRead.Add(rows, Tags(connector, scheme));

    /// <summary>Records records written by a sink.</summary>
    public static void RecordRowsWritten(string connector, string scheme, long rows) => RowsWritten.Add(rows, Tags(connector, scheme));

    /// <summary>Records a record that failed to map, and what was done with it.</summary>
    public static void RecordRowError(string connector, string scheme, string action)
    {
        var tags = Tags(connector, scheme);
        tags.Add("action", action);
        RowErrors.Add(1, tags);
    }

    /// <summary>Records messages settled with the broker, and how: <c>acknowledged</c>, <c>requeued</c>, <c>rejected</c> or <c>dead_lettered</c>.</summary>
    public static void RecordMessagesSettled(string connector, string outcome, long messages = 1)
    {
        var tags = Tags(connector, connector);
        tags.Add("outcome", outcome);
        MessagesSettled.Add(messages, tags);
    }

    internal static void RecordFileRead(string connector, StorageUri uri, long rows, long bytes)
    {
        var tags = Tags(connector, uri.Scheme.Value);
        RowsRead.Add(rows, tags);
        BytesRead.Add(bytes, tags);
        FilesRead.Add(1, tags);
    }

    internal static void RecordFileWritten(string connector, StorageUri uri, long rows, long bytes)
    {
        var tags = Tags(connector, uri.Scheme.Value);
        RowsWritten.Add(rows, tags);
        BytesWritten.Add(bytes, tags);
        FilesWritten.Add(1, tags);
    }

    internal static Activity? StartFileActivity(string name, string connector, StorageUri uri)
    {
        var activity = ActivitySource.StartActivity(name);

        if (activity is not null)
        {
            _ = activity.SetTag("npipeline.connector", connector);
            _ = activity.SetTag("storage.scheme", uri.Scheme.Value);
            _ = activity.SetTag("file.path", uri.Path);
        }

        return activity;
    }

    private static TagList Tags(string connector, string scheme) =>
        new()
        {
            { "connector", connector },
            { "storage.scheme", scheme },
        };
}
