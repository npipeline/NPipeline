namespace NPipeline.StorageProviders.Models;

/// <summary>Options for <see cref="Abstractions.IStorageProvider.OpenWriteAsync" />.</summary>
public sealed record StorageWriteOptions
{
    /// <summary>The MIME type to store with the object, where the store keeps one.</summary>
    public string? ContentType { get; init; }

    /// <summary>User metadata to store with the object, where the store keeps it.</summary>
    public IReadOnlyDictionary<string, string>? Metadata { get; init; }

    /// <summary>When <see langword="false" />, the write fails if the target exists. Requires <see cref="Abstractions.StorageCapabilities.ConditionalWrite" />.</summary>
    public bool Overwrite { get; init; } = true;

    /// <summary>Commit only if the target's current ETag matches. Requires <see cref="Abstractions.StorageCapabilities.ConditionalWrite" />.</summary>
    public string? IfMatch { get; init; }

    /// <summary>The expected object length, which lets a store choose a part size.</summary>
    public long? LengthHint { get; init; }

    internal bool IsConditional => !Overwrite || IfMatch is not null;
}
