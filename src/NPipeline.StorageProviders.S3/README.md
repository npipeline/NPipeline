# NPipeline.StorageProviders.S3

Shared S3 protocol abstractions and base implementation for NPipeline S3 storage providers. This package is a dependency of `NPipeline.StorageProviders.S3.Aws` and `NPipeline.StorageProviders.S3.Compatible`; it is not intended for direct use.

## Overview

`NPipeline.StorageProviders.S3` provides the provider-agnostic core that both S3 implementations build on:

- **`S3CoreStorageProvider`** - Abstract base class deriving from `StorageProvider` with read, write, list, delete, move, exists, and metadata support. Subclasses supply `Name` and `Schemes`
- **`S3ClientFactoryBase`** - Abstract factory that creates and caches `IAmazonS3` clients by configuration key
- **`S3CoreOptions`** - Base configuration with `MultipartUploadThresholdBytes` (default 64 MB)
- **`S3WriteStream`** - Streaming write implementation that switches to S3 multipart upload for large objects
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

**Multipart uploads** - `S3WriteStream` automatically switches to the S3 multipart upload API when the written content exceeds `MultipartUploadThresholdBytes`. The threshold is configurable per provider instance.

**Client caching** - `S3ClientFactoryBase` caches `IAmazonS3` instances by a configuration-derived key (region, endpoint, credentials) to avoid redundant client construction.

**Pagination** - `ListAsync` internally uses `ListObjectsV2` with continuation tokens, streaming items as pages arrive.

## S3CoreOptions

```csharp
public class S3CoreOptions
{
    // Files above this size are uploaded using the S3 multipart API. Default: 64 MB.
    public long MultipartUploadThresholdBytes { get; set; } = 64 * 1024 * 1024;
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
    protected override IAmazonS3 CreateClient(StorageUri uri) { ... }
    protected override string BuildCacheKey(StorageUri uri) { ... }
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
