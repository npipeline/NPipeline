---
title: "Custom Storage Provider"
description: "Derive from StorageProvider to integrate with unsupported storage systems."
order: 8
---

# Custom Storage Provider

> **Prerequisites:** [Storage Providers Overview](index.md)

If NPipeline doesn't have a built-in provider for your storage system, derive from `StorageProvider`. The base class validates arguments, observes cancellation and rejects operations your provider doesn't declare, so your code only handles the real work.

## Implementing a provider

Override `Name`, `Schemes` and `Capabilities`, then override the `...CoreAsync` method for each capability you declare:

```csharp
public sealed class FtpStorageProvider : StorageProvider
{
    private static readonly IReadOnlyList<StorageScheme> Supported = [new StorageScheme("ftp")];

    public override string Name => "FTP";

    public override IReadOnlyList<StorageScheme> Schemes => Supported;

    public override StorageCapabilities Capabilities =>
        StorageCapabilities.Read | StorageCapabilities.Write | StorageCapabilities.List | StorageCapabilities.Hierarchy;

    protected override async Task<Stream> OpenReadCoreAsync(StorageUri uri, CancellationToken ct)
    {
        var client = new FtpClient(uri.Host);
        await client.ConnectAsync(ct);
        return await client.OpenReadAsync(uri.Path, ct);
    }

    protected override async Task<StorageWriteStream> OpenWriteCoreAsync(
        StorageUri uri, StorageWriteOptions? options, CancellationToken ct)
    {
        // FtpWriteStream is a StorageWriteStream subclass; see "Writing" below.
        var client = new FtpClient(uri.Host);
        await client.ConnectAsync(ct);
        return new FtpWriteStream(client, uri.Path);
    }

    protected override async Task<StorageMetadata?> GetMetadataCoreAsync(StorageUri uri, CancellationToken ct)
    {
        var client = new FtpClient(uri.Host);
        await client.ConnectAsync(ct);
        var info = await client.GetInfoAsync(uri.Path, ct);
        return info is null ? null : new StorageMetadata { Size = info.Size, LastModified = info.Modified };
    }

    protected override async IAsyncEnumerable<StorageItem> ListCoreAsync(
        StorageUri directory, bool recursive, [EnumeratorCancellation] CancellationToken ct)
    {
        var client = new FtpClient(directory.Host);
        await client.ConnectAsync(ct);

        foreach (var item in await client.ListDirectoryAsync(directory.Path, recursive, ct))
        {
            // WithPath keeps the caller's host, port and parameters.
            yield return new StorageItem
            {
                Uri = directory.WithPath(item.IsDirectory ? item.Path + "/" : item.Path),
                Size = item.IsDirectory ? null : item.Size,
                LastModified = item.Modified,
                IsDirectory = item.IsDirectory,
            };
        }
    }
}
```

`ExistsAsync` defaults to "`GetMetadataAsync` returned something"; override `ExistsCoreAsync` if your store has a cheaper check. `ListCoreAsync` receives a directory that already ends with `/`.

## Capabilities

| Flag | Override |
|------|----------|
| `Read` | `OpenReadCoreAsync`, `GetMetadataCoreAsync` (and optionally `ExistsCoreAsync`) |
| `Write` | `OpenWriteCoreAsync` |
| `List` | `ListCoreAsync` |
| `Delete` | `DeleteCoreAsync` (must succeed when the object is missing) |
| `Move` | `MoveCoreAsync` (must overwrite the destination; a missing source throws `FileNotFoundException`) |
| `AtomicMove` | Declare it only when `MoveCoreAsync` is a single atomic rename |
| `Hierarchy` | Declare it for real directories; `ExistsAsync` is then `true` for a directory |
| `ConditionalWrite` | Honour `StorageWriteOptions.Overwrite` and `IfMatch` atomically in `CommitAsync` |

Declaring a capability you don't implement, or the reverse, is a bug: connectors read `Capabilities` to decide how to behave. For example, the DataLake manifest writer replaces the main manifest conditionally only when the provider declares `ConditionalWrite`.

## Writing

`OpenWriteCoreAsync` returns a `StorageWriteStream`, an abstract class in `NPipeline.StorageProviders.Abstractions` that derives from `Stream`. Callers write to it and then call `CommitAsync`. Your subclass must follow this contract:

- `CommitAsync` makes the bytes visible at the target atomically, and callers call it once, after the last write. Set the `ETag` property to the new ETag if the store returns one.
- Disposing the stream without a commit discards everything written. The target stays untouched, and an existing object stays as it was. Dispose never uploads.
- Upload failures surface from `CommitAsync`, using the caller's token, not from `Dispose`.

For an object store that has a multi-part upload API, derive from `ChunkedUploadStream` (also in `NPipeline.StorageProviders.Abstractions`). It uploads while the caller writes, so nothing goes to local disk: it collects the writes into pooled part buffers (`partSizeBytes` each), hands every full buffer to your `UploadPartAsync`, and keeps accepting writes while up to `maxConcurrency` parts upload. Memory use is about `partSizeBytes × (maxConcurrency + 1)`, and a writer that outruns the network waits instead of buffering without limit. You implement five methods:

```csharp
internal sealed class MyStoreWriteStream(MyClient client, string key)
    : ChunkedUploadStream(partSizeBytes: 8 * 1024 * 1024, maxConcurrency: 4)
{
    private string? _sessionId;
    private readonly ConcurrentDictionary<int, string> _partTags = new();

    // Called once, before the first part. Start the multi-part session here.
    protected override async Task BeginAsync(CancellationToken ct) =>
        _sessionId = await client.StartAsync(key, ct);

    // Parts upload concurrently and may finish in any order. `data` is valid until the task completes.
    protected override async Task UploadPartAsync(int partNumber, long offset, ReadOnlyMemory<byte> data, CancellationToken ct) =>
        _partTags[partNumber] = await client.UploadPartAsync(_sessionId!, partNumber, data, ct);

    // An object that fits in one part skips the session and goes in one request.
    protected override Task<string?> UploadSingleAsync(ReadOnlyMemory<byte> data, CancellationToken ct) =>
        client.PutAsync(key, data, ct);

    // Called by CommitAsync after every part has uploaded. Return the object's ETag, or null.
    protected override Task<string?> CompleteAsync(int partCount, long totalLength, CancellationToken ct) =>
        client.CompleteAsync(_sessionId!, _partTags.OrderBy(p => p.Key).Select(p => p.Value), ct);

    // Called when the stream is disposed without a commit, if BeginAsync ran. Failures are ignored.
    protected override Task AbortAsync() => client.AbortAsync(_sessionId!);
}
```

If a part fails, the stream faults: the next `WriteAsync` or `CommitAsync` throws that failure, and the other in-flight parts are cancelled. Override `PartSizeFor` to grow the part size for very large objects, so the object stays within the store's part limit.

If the store takes an object only in a single request, derive from `SpooledWriteStream` instead. It buffers the writes to a local temporary file, deletes the file when the stream is disposed, and calls your `UploadAsync` from `CommitAsync`:

```csharp
internal sealed class FtpWriteStream(FtpClient client, string path) : SpooledWriteStream("ftp-upload")
{
    protected override async Task<string?> UploadAsync(Stream content, CancellationToken ct)
    {
        await client.UploadAsync(path, content, ct);
        return null;   // return the object's ETag, or null if the store has none
    }
}
```

For a store that can rename, such as a file system or an SFTP server, write to a hidden sibling temporary file and rename it into place in `CommitAsync`. Delete the temporary file when the stream is disposed without a commit.

Declare `ConditionalWrite` only if the store enforces `StorageWriteOptions.Overwrite = false` and `IfMatch` atomically, for example with `If-None-Match` and `If-Match` headers. When a condition isn't met, throw `StoragePreconditionFailedException` (in `NPipeline.StorageProviders.Exceptions`) from `CommitAsync`. Don't declare the capability if you check the condition and then write in two steps: concurrent writers can slip between them.

## Caching clients

A provider that talks to a service through an SDK client usually needs one client per endpoint, not one per operation. Use `ClientCache<TKey, TClient>` (in `NPipeline.StorageProviders.Utilities`):

```csharp
private readonly record struct Endpoint(string Host, int? Port);   // routing data only

private readonly ClientCache<Endpoint, MyClient> _clients = new(limit: 100);

private MyClient GetClient(StorageUri uri) =>
    _clients.GetOrCreate(new Endpoint(uri.Host!, uri.Port), key => new MyClient(key.Host, key.Port, _options.Credentials));
```

A cache hit is one dictionary lookup, and the cache drops the least recently used client when it grows past the limit. Build the key from the URI's routing data (host, port, account, region) and never from a secret or a hash of one. Take credentials from your provider's options, so one set applies to every client and the key stays safe to log. An evicted client is not disposed, because an operation may still use it; `Dispose` on the cache disposes the clients that remain, so make your provider `IAsyncDisposable` and dispose the cache there.

## Error contract

Translate your store's errors into standard .NET exceptions, in one place:

| Situation | Throw |
|-----------|-------|
| Not found (read, move source) | `FileNotFoundException` (`GetMetadataCoreAsync` returns `null` instead) |
| Authentication or permission failure | `UnauthorizedAccessException` |
| Invalid bucket, container, key or path | `ArgumentException` |
| Any other failure | `IOException`, with the original exception as `InnerException` |

## Conformance tests

`NPipeline.Tests.Common` has an abstract `StorageProviderConformanceTests` class that checks the shared contract: object names with spaces, `#`, `%` and unicode; sizes around a part boundary; overwrite; missing objects; listing; delete and move; capabilities; and cancellation. Subclass it once for your provider, supply `CreateProviderAsync()` and `RootUri`, and override the switches for what your backend can't represent.

## Registration

### Manual

```csharp
var resolver = new StorageResolver([new FileSystemStorageProvider(), new FtpStorageProvider()]);
```

### Via DI

```csharp
services.AddStorageProvider<FtpStorageProvider>();   // singleton, registered once
services.AddStorageResolver();
```

Two providers that serve the same scheme make the resolver throw `ArgumentException`. Give the second one a different scheme, or pass it to a node through `Provider`.

## Next Steps

- [Storage Providers Overview](index.md) - built-in providers
- [Connectors](../connectors/index.md) - use your provider with file-based connectors
