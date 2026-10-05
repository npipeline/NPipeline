using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Tests;

/// <summary>A provider that serves the given schemes and declares the given capabilities, and records what reaches its core methods.</summary>
internal sealed class TestStorageProvider(string name, StorageCapabilities capabilities, params string[] schemes) : StorageProvider
{
    private readonly IReadOnlyList<StorageScheme> _schemes = [.. schemes.Select(s => new StorageScheme(s))];

    public List<string> Calls { get; } = [];

    public StorageUri? LastListed { get; private set; }

    public override string Name => name;

    public override IReadOnlyList<StorageScheme> Schemes => _schemes;

    public override StorageCapabilities Capabilities => capabilities;

    protected override Task<Stream> OpenReadCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        Calls.Add("read");
        return Task.FromResult<Stream>(new MemoryStream());
    }

    protected override Task<StorageWriteStream> OpenWriteCoreAsync(StorageUri uri, StorageWriteOptions? options, CancellationToken cancellationToken)
    {
        Calls.Add("write");
        return Task.FromResult<StorageWriteStream>(new PassThroughWriteStream(new MemoryStream()));
    }

    protected override Task<StorageMetadata?> GetMetadataCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        Calls.Add("metadata");
        return Task.FromResult<StorageMetadata?>(uri.Name == "present" ? new StorageMetadata { Size = 1 } : null);
    }

    protected override async IAsyncEnumerable<StorageItem> ListCoreAsync(StorageUri directory, bool recursive, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Calls.Add("list");
        LastListed = directory;
        await Task.CompletedTask.ConfigureAwait(false);
        yield break;
    }

    protected override Task DeleteCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        Calls.Add("delete");
        return Task.CompletedTask;
    }

    protected override Task MoveCoreAsync(StorageUri source, StorageUri destination, CancellationToken cancellationToken)
    {
        Calls.Add("move");
        return Task.CompletedTask;
    }
}
