using System.Collections.Concurrent;
using Amazon.S3;
using Amazon.S3.Model;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.StorageProviders.S3;

/// <summary>
///     A write stream that uploads to S3 while it is being written: a single <c>PutObject</c> for an object that fits in one
///     part, otherwise a multipart upload whose parts are sent as they fill. Nothing becomes visible until
///     <see cref="StorageWriteStream.CommitAsync" /> completes the upload, and disposing without committing aborts it.
/// </summary>
public sealed class S3WriteStream : ChunkedUploadStream
{
    /// <summary>S3 allows at most 10,000 parts per upload.</summary>
    private const int MaxParts = 10_000;

    private const int MaxGrownPartSize = 128 * 1024 * 1024;
    private const int PartsPerGrowthStep = 1_000;

    private readonly string _bucket;
    private readonly string? _contentType;
    private readonly string? _ifMatch;
    private readonly string _key;
    private readonly long? _lengthHint;
    private readonly bool _overwrite;
    private readonly ConcurrentDictionary<int, string> _partETags = new();
    private readonly IAmazonS3 _s3Client;
    private string? _uploadId;

    /// <summary>
    ///     Initializes a new instance of the <see cref="S3WriteStream" /> class.
    /// </summary>
    /// <param name="s3Client">The Amazon S3 client.</param>
    /// <param name="bucket">The S3 bucket name.</param>
    /// <param name="key">The S3 object key.</param>
    /// <param name="contentType">Optional content type for the upload.</param>
    /// <param name="partSizeBytes">Size of each part. At least 5 MiB. Default is 8 MiB.</param>
    /// <param name="maxConcurrency">The most parts uploading at the same time. Default is 4.</param>
    /// <param name="lengthHint">The expected object length, used to choose a part size that stays within 10,000 parts.</param>
    /// <param name="ifMatch">Commit only if the object's current ETag matches.</param>
    /// <param name="overwrite">When <see langword="false" />, commit fails if the object exists.</param>
    public S3WriteStream(
        IAmazonS3 s3Client,
        string bucket,
        string key,
        string? contentType = null,
        int partSizeBytes = 8 * 1024 * 1024,
        int maxConcurrency = 4,
        long? lengthHint = null,
        string? ifMatch = null,
        bool overwrite = true)
        : base(ChoosePartSize(partSizeBytes, lengthHint), maxConcurrency)
    {
        _s3Client = s3Client ?? throw new ArgumentNullException(nameof(s3Client));
        _bucket = bucket ?? throw new ArgumentNullException(nameof(bucket));
        _key = key ?? throw new ArgumentNullException(nameof(key));
        _contentType = contentType;
        _ifMatch = ifMatch;
        _overwrite = overwrite;
        _lengthHint = lengthHint;
    }

    /// <inheritdoc />
    protected override int PartSizeFor(int partNumber)
    {
        // Without a length hint the part size grows every 1,000 parts, so objects beyond 10,000 fixed-size parts still fit.
        if (_lengthHint is not null || partNumber <= PartsPerGrowthStep)
            return PartSizeBytes;

        var step = Math.Min((partNumber - 1) / PartsPerGrowthStep, 5);
        return (int)Math.Min((long)PartSizeBytes << step, MaxGrownPartSize);
    }

    /// <inheritdoc />
    protected override async Task BeginAsync(CancellationToken cancellationToken)
    {
        try
        {
            var response = await _s3Client.InitiateMultipartUploadAsync(
                new InitiateMultipartUploadRequest { BucketName = _bucket, Key = _key, ContentType = _contentType },
                cancellationToken).ConfigureAwait(false);

            _uploadId = response.UploadId;
        }
        catch (AmazonS3Exception ex)
        {
            throw S3Errors.Translate(ex, _bucket, _key);
        }
    }

    /// <inheritdoc />
    protected override async Task UploadPartAsync(int partNumber, long offset, ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
    {
        if (partNumber > MaxParts)
            throw new IOException($"The object needs more than {MaxParts} parts. Set StorageWriteOptions.LengthHint or raise PartSizeBytes.");

        try
        {
            using var body = AsStream(data);

            var response = await _s3Client.UploadPartAsync(
                new UploadPartRequest
                {
                    BucketName = _bucket,
                    Key = _key,
                    UploadId = _uploadId,
                    PartNumber = partNumber,
                    PartSize = data.Length,
                    InputStream = body,
                },
                cancellationToken).ConfigureAwait(false);

            _partETags[partNumber] = response.ETag;
        }
        catch (AmazonS3Exception ex)
        {
            throw S3Errors.Translate(ex, _bucket, _key);
        }
    }

    /// <inheritdoc />
    protected override async Task<string?> UploadSingleAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
    {
        try
        {
            using var body = AsStream(data);

            var request = new PutObjectRequest
            {
                BucketName = _bucket,
                Key = _key,
                InputStream = body,
                AutoCloseStream = false,
                UseChunkEncoding = false,
            };

            if (_ifMatch is not null)
                request.IfMatch = _ifMatch;
            else if (!_overwrite)
                request.IfNoneMatch = "*";

            if (!string.IsNullOrEmpty(_contentType))
                request.ContentType = _contentType;

            var response = await _s3Client.PutObjectAsync(request, cancellationToken).ConfigureAwait(false);

            return response?.ETag;
        }
        catch (AmazonS3Exception ex)
        {
            throw S3Errors.Translate(ex, _bucket, _key);
        }
    }

    /// <inheritdoc />
    protected override async Task<string?> CompleteAsync(int partCount, long totalLength, CancellationToken cancellationToken)
    {
        var request = new CompleteMultipartUploadRequest
        {
            BucketName = _bucket,
            Key = _key,
            UploadId = _uploadId,
            PartETags = _partETags.OrderBy(p => p.Key).Select(p => new PartETag(p.Key, p.Value)).ToList(),
        };

        if (_ifMatch is not null)
            request.IfMatch = _ifMatch;
        else if (!_overwrite)
            request.IfNoneMatch = "*";

        try
        {
            var completed = await _s3Client.CompleteMultipartUploadAsync(request, cancellationToken).ConfigureAwait(false);

            return completed?.ETag;
        }
        catch (AmazonS3Exception ex)
        {
            throw S3Errors.Translate(ex, _bucket, _key);
        }
    }

    /// <inheritdoc />
    protected override async Task AbortAsync()
    {
        if (_uploadId is null)
            return;

        _ = await _s3Client.AbortMultipartUploadAsync(
            new AbortMultipartUploadRequest { BucketName = _bucket, Key = _key, UploadId = _uploadId },
            CancellationToken.None).ConfigureAwait(false);
    }

    private static int ChoosePartSize(int partSizeBytes, long? lengthHint)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(partSizeBytes, S3CoreOptions.MinPartSizeBytes);

        if (lengthHint is not > 0)
            return partSizeBytes;

        // The smallest size that keeps the object within 10,000 parts, rounded up to a whole MiB.
        var needed = (lengthHint.Value + MaxParts - 1) / MaxParts;
        var rounded = (needed + (1024 * 1024) - 1) / (1024 * 1024) * (1024 * 1024);

        return (int)Math.Min(Math.Max(partSizeBytes, rounded), MaxGrownPartSize);
    }
}
