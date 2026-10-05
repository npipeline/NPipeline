namespace NPipeline.StorageProviders.Abstractions;

/// <summary>The operations a storage provider supports. Operations outside the declared set throw <see cref="Exceptions.UnsupportedStorageCapabilityException" />.</summary>
[Flags]
public enum StorageCapabilities
{
    /// <summary>The provider supports nothing through the file API (for example a database provider).</summary>
    None = 0,

    /// <summary>Supports <c>OpenReadAsync</c>, <c>GetMetadataAsync</c> and <c>ExistsAsync</c>.</summary>
    Read = 1 << 0,

    /// <summary>Supports <c>OpenWriteAsync</c>.</summary>
    Write = 1 << 1,

    /// <summary>Supports <c>ListAsync</c>.</summary>
    List = 1 << 2,

    /// <summary>Supports <c>DeleteAsync</c>.</summary>
    Delete = 1 << 3,

    /// <summary><c>MoveAsync</c> is implemented. It can be a copy followed by a delete.</summary>
    Move = 1 << 4,

    /// <summary><c>MoveAsync</c> is a single atomic rename.</summary>
    AtomicMove = 1 << 5,

    /// <summary>The store has real directories (file system, ADLS, SFTP) rather than key prefixes.</summary>
    Hierarchy = 1 << 6,

    /// <summary>Writes honour <see cref="Models.StorageWriteOptions.Overwrite" /> and <see cref="Models.StorageWriteOptions.IfMatch" /> atomically.</summary>
    ConditionalWrite = 1 << 7,
}
