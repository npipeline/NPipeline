namespace Shared;

/// <summary>
///     Represents the structure of data at the pipeline source.
/// </summary>
/// <param name="Id">The unique identifier for the data record.</param>
/// <param name="Name">The name or description associated with the record.</param>
public record SourceData(int Id, string Name);

/// <summary>
///     Represents the structure of data after processing through the pipeline.
/// </summary>
/// <param name="Id">The unique identifier for the data record.</param>
/// <param name="Name">The name or description associated with the record.</param>
public record TargetData(int Id, string Name);
