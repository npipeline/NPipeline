using System.Collections.Concurrent;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.StorageProviders.Azure;

/// <summary>
///     A write stream that uploads a block blob while it is being written: one request for a blob that fits in a single
///     block, otherwise blocks staged as they fill. Nothing becomes visible until
///     <see cref="StorageWriteStream.CommitAsync" /> commits the block list. Disposing without committing abandons the staged
///     blocks, which the service discards after a week.
/// </summary>
public sealed class AzureBlobWriteStream : ChunkedUploadStream
{
    /// <summary>A block blob holds at most 50,000 blocks.</summary>
    private const int MaxBlocks = 50_000;

    private const int MaxGrownPartSize = 128 * 1024 * 1024;
    private const int PartsPerGrowthStep = 10_000;

    private readonly string _blob;
    private readonly BlockBlobClient _blobClient;
    private readonly string _container;
    private readonly string? _contentType;
    private readonly string? _ifMatch;
    private readonly long? _lengthHint;
    private readonly bool _overwrite;
    private readonly ConcurrentDictionary<int, string> _blockIds = new();
    private readonly Func<RequestFailedException, Exception> _translate;

    /// <summary>
    ///     Initializes a new instance of the <see cref="AzureBlobWriteStream" /> class.
    /// </summary>
    /// <param name="blobServiceClient">The Azure Blob Service client.</param>
    /// <param name="container">The Azure container name.</param>
    /// <param name="blob">The Azure blob name.</param>
    /// <param name="contentType">Optional content type for the upload.</param>
    /// <param name="partSizeBytes">Size of each block. Default is 8 MiB.</param>
    /// <param name="maxConcurrency">The most blocks uploading at the same time. Default is 4.</param>
    /// <param name="lengthHint">The expected blob length, used to choose a block size that stays within 50,000 blocks.</param>
    /// <param name="ifMatch">Commit only if the blob's current ETag matches.</param>
    /// <param name="overwrite">When <see langword="false" />, commit fails if the blob exists.</param>
    /// <param name="translateError">Maps a service failure to the exception to throw. Defaults to the Azure Blob mapping.</param>
    public AzureBlobWriteStream(
        BlobServiceClient blobServiceClient,
        string container,
        string blob,
        string? contentType = null,
        int partSizeBytes = 8 * 1024 * 1024,
        int maxConcurrency = 4,
        long? lengthHint = null,
        string? ifMatch = null,
        bool overwrite = true,
        Func<RequestFailedException, Exception>? translateError = null)
        : this(
            GetBlockBlobClient(blobServiceClient, container, blob),
            container,
            blob,
            contentType,
            partSizeBytes,
            maxConcurrency,
            lengthHint,
            ifMatch,
            overwrite,
            translateError)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="AzureBlobWriteStream" /> class over a block blob client. This is the
    ///     seam that lets tests substitute the client.
    /// </summary>
    internal AzureBlobWriteStream(
        BlockBlobClient blobClient,
        string container,
        string blob,
        string? contentType,
        int partSizeBytes,
        int maxConcurrency,
        long? lengthHint,
        string? ifMatch,
        bool overwrite,
        Func<RequestFailedException, Exception>? translateError)
        : base(ChoosePartSize(partSizeBytes, lengthHint), maxConcurrency)
    {
        _blobClient = blobClient ?? throw new ArgumentNullException(nameof(blobClient));
        _container = container ?? throw new ArgumentNullException(nameof(container));
        _blob = blob ?? throw new ArgumentNullException(nameof(blob));
        _contentType = contentType;
        _ifMatch = ifMatch;
        _overwrite = overwrite;
        _lengthHint = lengthHint;
        _translate = translateError ?? (ex => AzureErrors.Translate(ex, container, blob));
    }

    private static BlockBlobClient GetBlockBlobClient(BlobServiceClient blobServiceClient, string container, string blob)
    {
        ArgumentNullException.ThrowIfNull(blobServiceClient);
        ArgumentNullException.ThrowIfNull(container);
        ArgumentNullException.ThrowIfNull(blob);

        return blobServiceClient.GetBlobContainerClient(container).GetBlockBlobClient(blob);
    }

    /// <inheritdoc />
    protected override int PartSizeFor(int partNumber)
    {
        if (_lengthHint is not null || partNumber <= PartsPerGrowthStep)
            return PartSizeBytes;

        var step = Math.Min((partNumber - 1) / PartsPerGrowthStep, 4);
        return (int)Math.Min((long)PartSizeBytes << step, MaxGrownPartSize);
    }

    /// <inheritdoc />
    protected override async Task UploadPartAsync(int partNumber, long offset, ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
    {
        if (partNumber > MaxBlocks)
            throw new IOException($"The blob needs more than {MaxBlocks} blocks. Set StorageWriteOptions.LengthHint or raise PartSizeBytes.");

        // Block ids of one blob must all have the same length.
        var blockId = Convert.ToBase64String(BitConverter.GetBytes((long)partNumber));

        try
        {
            using var body = AsStream(data);
            _ = await _blobClient.StageBlockAsync(blockId, body, cancellationToken: cancellationToken).ConfigureAwait(false);
            _blockIds[partNumber] = blockId;
        }
        catch (RequestFailedException ex)
        {
            throw _translate(ex);
        }
    }

    /// <inheritdoc />
    protected override async Task<string?> UploadSingleAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
    {
        try
        {
            using var body = AsStream(data);

            var response = await _blobClient.UploadAsync(
                body,
                new BlobUploadOptions { HttpHeaders = CreateHeaders(), Conditions = CreateConditions() },
                cancellationToken).ConfigureAwait(false);

            return ETagOf(response?.Value?.ETag);
        }
        catch (RequestFailedException ex)
        {
            throw _translate(ex);
        }
    }

    /// <inheritdoc />
    protected override async Task<string?> CompleteAsync(int partCount, long totalLength, CancellationToken cancellationToken)
    {
        try
        {
            var response = await _blobClient.CommitBlockListAsync(
                _blockIds.OrderBy(b => b.Key).Select(b => b.Value),
                new CommitBlockListOptions { HttpHeaders = CreateHeaders(), Conditions = CreateConditions() },
                cancellationToken).ConfigureAwait(false);

            return ETagOf(response?.Value?.ETag);
        }
        catch (RequestFailedException ex)
        {
            throw _translate(ex);
        }
    }

    private BlobHttpHeaders? CreateHeaders() =>
        string.IsNullOrEmpty(_contentType)
            ? null
            : new BlobHttpHeaders { ContentType = _contentType };

    private BlobRequestConditions? CreateConditions()
    {
        if (_ifMatch is not null)
            return new BlobRequestConditions { IfMatch = new global::Azure.ETag(_ifMatch) };

        return _overwrite
            ? null
            : new BlobRequestConditions { IfNoneMatch = global::Azure.ETag.All };
    }

    private static string? ETagOf(global::Azure.ETag? etag)
    {
        var text = etag?.ToString();

        return string.IsNullOrEmpty(text) ? null : text;
    }

    private static int ChoosePartSize(int partSizeBytes, long? lengthHint)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(partSizeBytes);

        if (lengthHint is not > 0)
            return partSizeBytes;

        // The smallest size that keeps the blob within 50,000 blocks, rounded up to a whole MiB.
        var needed = (lengthHint.Value + MaxBlocks - 1) / MaxBlocks;
        var rounded = (needed + (1024 * 1024) - 1) / (1024 * 1024) * (1024 * 1024);

        return (int)Math.Min(Math.Max(partSizeBytes, rounded), MaxGrownPartSize);
    }
}
