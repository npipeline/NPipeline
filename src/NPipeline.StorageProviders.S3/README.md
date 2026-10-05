# NPipeline.StorageProviders.S3

Shared S3 protocol abstractions and base implementation for NPipeline S3 storage providers. This package is a dependency of `NPipeline.StorageProviders.S3.Aws` and `NPipeline.StorageProviders.S3.Compatible`; it is not intended for direct use.

## Overview

`NPipeline.StorageProviders.S3` provides the provider-agnostic core that both S3 implementations build on:

- **`S3CoreStorageProvider`** - Abstract base class deriving from `StorageProvider` with read, write, list, delete, move, exists, and metadata support. Subclasses supply `Name` and `Schemes`
- **`S3ClientFactoryBase`** - Abstract factory that creates and caches `IAmazonS3` clients, one per endpoint (`S3EndpointKey`)
- **`S3CoreOptions`** - Base configuration with `PartSizeBytes` (default 8 MiB), `MaxConcurrency` (default 4) and `ClientCacheSizeLimit`
- **`S3WriteStream`** - Streaming write implementation built on `ChunkedUploadStream`: one `PutObject` for an object that fits in a part, otherwise a multipart upload whose parts are sent as they fill
- **Error translation** - One internal translator maps the HTTP status first (404 to `FileNotFoundException`, 401/403 to `UnauthorizedAccessException`, 400 to `ArgumentException`) and refines by error code; other failures become `IOException` with the `AmazonS3Exception` as inner exception

## URI Scheme

Both concrete providers use the `s3://` scheme by default (the S3-compatible provider can be registered under other schemes):

```
s3://bucket-name/key/path
```

| Component | Description |
|-----------|-------------|
| `bucket-name` | S3 bucket name (URI host) |
| `key/path` | Object key (URI path, leading `/` is stripped) |

## Key Behaviours

**Flat storage** - S3 has no real directory hierarchy. Prefixes simulate folders. The providers declare `Read | Write | List | Delete | Move`, and not `Hierarchy`.

**Delete and move** - `DeleteAsync` calls `DeleteObject` and succeeds for a missing key. `MoveAsync` copies with `CopyObject` (or a multipart `UploadPartCopy` for objects over 5 GiB) and then deletes the source, so it overwrites the destination but is not atomic (`AtomicMove` is not declared). It throws `FileNotFoundException` when the source is missing.

**Listing** - The directory URI ends with `/`, so `logs` does not match `logs-archive/`. A non-recursive listing yields objects and prefix entries (`IsDirectory = true`, `Size` and `LastModified` null). A recursive listing yields only objects. Listed URIs keep the caller's host and parameters. A missing bucket yields nothing.

**Timestamps** - `LastModified` and `Size` are `null` when S3 does not report them.

**Read streams** - The read stream reports `Length` from `ContentLength`.

**Streaming uploads** - `S3WriteStream` uploads while the caller writes. Part buffers come from `ArrayPool<byte>`, at most `MaxConcurrency` parts are in flight, and memory is about `PartSizeBytes × (MaxConcurrency + 1)`. Nothing goes to local disk. `CommitAsync` completes the upload; disposing without committing aborts it. With `StorageWriteOptions.LengthHint`, the part size rises so the upload stays within S3's 10,000 parts; without a hint, it grows after part 1,000.

**Client caching** - `S3ClientFactoryBase` caches `IAmazonS3` instances by endpoint (region, service URL, addressing style), with least-recently-used eviction. The key never includes a credential: credentials belong to the factory's options, so a lookup is one dictionary hit.

**Pagination** - `ListAsync` internally uses `ListObjectsV2` with continuation tokens, streaming items as pages arrive.

## S3CoreOptions

```csharp
public class S3CoreOptions
{
    // Size of each upload part, at least 5 MiB. Default: 8 MiB.
    public int PartSizeBytes { get; set; }

    // Parts of one object that upload at the same time. Default: 4.
    public int MaxConcurrency { get; set; }

    // Endpoint clients kept in the cache. Default: 100.
    public int ClientCacheSizeLimit { get; set; }
}
```

## Implementing a Custom S3 Provider

Extend `S3CoreStorageProvider` and provide a concrete `S3ClientFactoryBase`:

```csharp
public class MyS3Provider : S3CoreStorageProvider
{
    public MyS3Provider(MyClientFactory factory, S3CoreOptions options)
        : base(factory, options) { }

    public override string Name => "My S3";

    public override IReadOnlyList<StorageScheme> Schemes { get; } = [StorageScheme.S3];
}

public class MyClientFactory : S3ClientFactoryBase
{
    protected override S3EndpointKey GetEndpoint(StorageUri uri) { ... }   // routing data only, never a secret
    protected override IAmazonS3 CreateClient(S3EndpointKey endpoint) { ... }
}
```

## Dependencies

- `AWSSDK.S3` - Amazon S3 client (`IAmazonS3`, `AmazonS3Exception`)
- `AWSSDK.Core` - AWS SDK core types (`AWSCredentials`, credential chain)
- `NPipeline.StorageProviders` - `IStorageProvider`, `StorageUri`, `StorageItem`, `StorageMetadata`
- `NPipeline` - Core pipeline engine

## Requirements

- .NET 8.0, 9.0, or 10.0

## Related Packages

- **[NPipeline.StorageProviders.S3.Aws](https://www.nuget.org/packages/NPipeline.StorageProviders.S3.Aws)** - AWS S3 with IAM credential chain and per-URI region overrides
- **[NPipeline.StorageProviders.S3.Compatible](https://www.nuget.org/packages/NPipeline.StorageProviders.S3.Compatible)** - MinIO, Cloudflare R2, DigitalOcean Spaces, and other S3-compatible services

## License

This package is licensed under the [Business Source License 1.1](LICENSE.txt).

**Free for non-production use.** Production use is free for organizations with 4 or fewer developers and annual revenue of $5M AUD or less. Larger organizations require a [commercial license](https://npipeline.com). This license automatically converts to MIT two years after each release.
