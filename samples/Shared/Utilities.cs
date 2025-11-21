namespace Shared;

/// <summary>
///     Utility class for creating test data for samples.
/// </summary>
public static class TestDataGenerator
{
    private static readonly Random Random = new();

    /// <summary>
    ///     Generates a collection of SourceData records.
    /// </summary>
    /// <param name="count">The number of records to generate.</param>
    /// <param name="contentPrefix">Optional prefix for the content field.</param>
    /// <returns>A collection of SourceData records.</returns>
    public static IEnumerable<SourceData> GenerateSourceData(int count, string contentPrefix = "Data")
    {
        for (var i = 1; i <= count; i++)
        {
            yield return new SourceData(
                $"item-{i:D3}",
                $"{contentPrefix} item {i}",
                DateTime.UtcNow.AddSeconds(-Random.Next(0, 3600))
            );
        }
    }

    /// <summary>
    ///     Generates a single SourceData record.
    /// </summary>
    /// <param name="id">The ID for the record.</param>
    /// <param name="content">The content for the record.</param>
    /// <param name="timestamp">Optional timestamp (defaults to now).</param>
    /// <returns>A SourceData record.</returns>
    public static SourceData CreateSourceData(string id, string content, DateTime? timestamp = null)
    {
        return new SourceData(
            id,
            content,
            timestamp ?? DateTime.UtcNow
        );
    }

    /// <summary>
    ///     Generates a collection of ProcessedData records from SourceData.
    /// </summary>
    /// <param name="sourceData">The source SourceData records.</param>
    /// <param name="processedBy">The identifier of what processed the data.</param>
    /// <returns>A collection of ProcessedData records.</returns>
    public static IEnumerable<ProcessedData> GenerateProcessedData(IEnumerable<SourceData> sourceData, string processedBy = "SampleProcessor")
    {
        foreach (var data in sourceData)
        {
            yield return CreateProcessedData(data, processedBy);
        }
    }

    /// <summary>
    ///     Creates a ProcessedData record from SourceData.
    /// </summary>
    /// <param name="sourceData">The source SourceData record.</param>
    /// <param name="processedBy">The identifier of what processed the data.</param>
    /// <param name="processedAt">Optional processing timestamp (defaults to now).</param>
    /// <returns>A ProcessedData record.</returns>
    public static ProcessedData CreateProcessedData(SourceData sourceData, string processedBy, DateTime? processedAt = null)
    {
        return new ProcessedData(
            sourceData.Id,
            sourceData.Content,
            processedBy,
            processedAt ?? DateTime.UtcNow,
            sourceData.Timestamp
        );
    }
}
