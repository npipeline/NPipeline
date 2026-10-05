using System.Runtime.CompilerServices;
using NPipeline.StorageProviders.Exceptions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Abstractions;

/// <summary>
///     Base class for <see cref="IStorageProvider" /> implementations. It validates arguments, observes cancellation,
///     rejects operations the provider does not declare in <see cref="Capabilities" />, and then calls the matching
///     <c>...CoreAsync</c> method, which a provider overrides for each capability it declares.
/// </summary>
public abstract class StorageProvider : IStorageProvider
{
    /// <inheritdoc />
    public abstract string Name { get; }

    /// <inheritdoc />
    public abstract IReadOnlyList<StorageScheme> Schemes { get; }

    /// <inheritdoc />
    public abstract StorageCapabilities Capabilities { get; }

    /// <inheritdoc />
    public Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        Begin(uri, StorageCapabilities.Read, "read", cancellationToken);
        return OpenReadCoreAsync(uri, cancellationToken);
    }

    /// <inheritdoc />
    public Task<StorageWriteStream> OpenWriteAsync(StorageUri uri, StorageWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        Begin(uri, StorageCapabilities.Write, "write", cancellationToken);

        if (options?.IsConditional == true && !Capabilities.HasFlag(StorageCapabilities.ConditionalWrite))
            throw new UnsupportedStorageCapabilityException(uri, "conditional write", Name);

        return OpenWriteCoreAsync(uri, options, cancellationToken);
    }

    /// <inheritdoc />
    public Task<StorageMetadata?> GetMetadataAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        Begin(uri, StorageCapabilities.Read, "metadata", cancellationToken);
        return GetMetadataCoreAsync(uri, cancellationToken);
    }

    /// <inheritdoc />
    public Task<bool> ExistsAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        Begin(uri, StorageCapabilities.Read, "exists", cancellationToken);
        return ExistsCoreAsync(uri, cancellationToken);
    }

    /// <inheritdoc />
    public IAsyncEnumerable<StorageItem> ListAsync(StorageUri directory, bool recursive = false, CancellationToken cancellationToken = default)
    {
        Begin(directory, StorageCapabilities.List, "list", cancellationToken);
        return ListCoreAsync(directory.IsDirectory ? directory : directory.WithPath(directory.Path + "/"), recursive, cancellationToken);
    }

    /// <inheritdoc />
    public Task DeleteAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        Begin(uri, StorageCapabilities.Delete, "delete", cancellationToken);
        return DeleteCoreAsync(uri, cancellationToken);
    }

    /// <inheritdoc />
    public Task MoveAsync(StorageUri source, StorageUri destination, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(destination);
        Begin(source, StorageCapabilities.Move, "move", cancellationToken);
        return MoveCoreAsync(source, destination, cancellationToken);
    }

    /// <summary>Opens a readable stream. Called after validation, for providers that declare <see cref="StorageCapabilities.Read" />.</summary>
    protected virtual Task<Stream> OpenReadCoreAsync(StorageUri uri, CancellationToken cancellationToken) =>
        throw Unsupported(uri, "read");

    /// <summary>Opens a writable stream. Called after validation, for providers that declare <see cref="StorageCapabilities.Write" />.</summary>
    protected virtual Task<StorageWriteStream> OpenWriteCoreAsync(StorageUri uri, StorageWriteOptions? options, CancellationToken cancellationToken) =>
        throw Unsupported(uri, "write");

    /// <summary>Returns metadata, or <see langword="null" /> when the object is missing. Called for providers that declare <see cref="StorageCapabilities.Read" />.</summary>
    protected virtual Task<StorageMetadata?> GetMetadataCoreAsync(StorageUri uri, CancellationToken cancellationToken) =>
        throw Unsupported(uri, "metadata");

    /// <summary>Checks existence. The default is <c>GetMetadataAsync(...) is not null</c>.</summary>
    protected virtual async Task<bool> ExistsCoreAsync(StorageUri uri, CancellationToken cancellationToken) =>
        await GetMetadataCoreAsync(uri, cancellationToken).ConfigureAwait(false) is not null;

    /// <summary>Lists a directory. <paramref name="directory" /> already ends with <c>/</c>. Called for providers that declare <see cref="StorageCapabilities.List" />.</summary>
    protected virtual IAsyncEnumerable<StorageItem> ListCoreAsync(StorageUri directory, bool recursive, CancellationToken cancellationToken) =>
        throw Unsupported(directory, "list");

    /// <summary>Deletes an object. Called for providers that declare <see cref="StorageCapabilities.Delete" />.</summary>
    protected virtual Task DeleteCoreAsync(StorageUri uri, CancellationToken cancellationToken) =>
        throw Unsupported(uri, "delete");

    /// <summary>Moves an object. Called for providers that declare <see cref="StorageCapabilities.Move" />.</summary>
    protected virtual Task MoveCoreAsync(StorageUri source, StorageUri destination, CancellationToken cancellationToken) =>
        throw Unsupported(source, "move");

    /// <summary>Creates the exception for an operation the provider does not support.</summary>
    protected UnsupportedStorageCapabilityException Unsupported(StorageUri uri, string capability) => new(uri, capability, Name);

    private void Begin(StorageUri uri, StorageCapabilities required, string operation, CancellationToken cancellationToken,
        [CallerArgumentExpression(nameof(uri))] string? parameterName = null)
    {
        ArgumentNullException.ThrowIfNull(uri, parameterName);
        cancellationToken.ThrowIfCancellationRequested();

        if (!Capabilities.HasFlag(required))
            throw Unsupported(uri, operation);
    }
}
