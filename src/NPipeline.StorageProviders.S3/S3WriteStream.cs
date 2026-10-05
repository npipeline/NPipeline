using Amazon.S3;
using Amazon.S3.Model;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.StorageProviders.S3;

/// <summary>
///     A write stream that buffers to a local file and uploads the object in <see cref="StorageWriteStream.CommitAsync" />:
///     a single <c>PutObject</c>, or a multipart upload above the configured threshold. Disposing without committing uploads nothing.
/// </summary>
public sealed class S3WriteStream : SpooledWriteStream
{
    private const int DefaultPartSize = 8 * 1024 * 1024; // 8 MB parts
    private const int MaxConcurrentUploads = 4;

    private readonly string _bucket;
    private readonly string? _contentType;
    private readonly string? _ifMatch;
    private readonly string _key;
    private readonly long _multipartUploadThreshold;
    private readonly int _partSize;
    private readonly bool _overwrite;
    private readonly object _readLock = new();
    private readonly IAmazonS3 _s3Client;
    private Stream? _content;

    /// <summary>
    ///     Initializes a new instance of the <see cref="S3WriteStream" /> class.
    /// </summary>
    /// <param name="s3Client">The Amazon S3 client.</param>
    /// <param name="bucket">The S3 bucket name.</param>
    /// <param name="key">The S3 object key.</param>
    /// <param name="contentType">Optional content type for the upload.</param>
    /// <param name="multipartUploadThreshold">Threshold in bytes for using multipart upload. Default is 64 MB.</param>
    /// <param name="partSize">Size of each part for multipart upload. Default is 8 MB.</param>
    /// <param name="ifMatch">Commit only if the object's current ETag matches.</param>
    /// <param name="overwrite">When <see langword="false" />, commit fails if the object exists.</param>
    public S3WriteStream(
        IAmazonS3 s3Client,
        string bucket,
        string key,
        string? contentType = null,
        long multipartUploadThreshold = 64 * 1024 * 1024,
        int partSize = DefaultPartSize,
        string? ifMatch = null,
        bool overwrite = true)
        : base("s3-upload")
    {
        _s3Client = s3Client ?? throw new ArgumentNullException(nameof(s3Client));
        _bucket = bucket ?? throw new ArgumentNullException(nameof(bucket));
        _key = key ?? throw new ArgumentNullException(nameof(key));
        _contentType = contentType;
        _ifMatch = ifMatch;
        _overwrite = overwrite;
        _multipartUploadThreshold = multipartUploadThreshold;
        _partSize = Math.Max(partSize, 5 * 1024 * 1024); // Minimum 5 MB per S3 requirements
    }

    /// <inheritdoc />
    protected override async Task<string?> UploadAsync(Stream content, CancellationToken cancellationToken)
    {
        _content = content;
        var contentLength = content.Length;

        try
        {
            return contentLength > 0 && contentLength >= _multipartUploadThreshold
                ? await UploadMultipartAsync(contentLength, cancellationToken).ConfigureAwait(false)
                : await UploadSingleAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (AmazonS3Exception ex)
        {
            throw S3Errors.Translate(ex, _bucket, _key);
        }
    }

    /// <summary>
    ///     Uploads the content using a single PutObject request.
    /// </summary>
    private async Task<string?> UploadSingleAsync(CancellationToken cancellationToken)
    {
        var request = new PutObjectRequest
        {
            BucketName = _bucket,
            Key = _key,
            InputStream = _content,
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

    /// <summary>
    ///     Uploads the content using S3 multipart upload.
    /// </summary>
    /// <param name="contentLength">The total length of the content to upload.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    private async Task<string?> UploadMultipartAsync(long contentLength, CancellationToken cancellationToken)
    {
        // Initiate multipart upload
        var initiateRequest = new InitiateMultipartUploadRequest
        {
            BucketName = _bucket,
            Key = _key,
            ContentType = _contentType,
        };

        var initiateResponse = await _s3Client.InitiateMultipartUploadAsync(initiateRequest, cancellationToken).ConfigureAwait(false);
        var uploadId = initiateResponse.UploadId;
        var parts = new List<PartETag>();

        try
        {
            // Calculate part boundaries
            var partCount = (int)Math.Ceiling((double)contentLength / _partSize);

            // Upload parts, optionally in parallel
            using var semaphore = new SemaphoreSlim(MaxConcurrentUploads);
            var uploadTasks = new List<Task<PartETag>>();

            for (var partNumber = 1; partNumber <= partCount; partNumber++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var currentPartNumber = partNumber;
                var task = UploadPartAsync(currentPartNumber, contentLength, uploadId, semaphore, cancellationToken);
                uploadTasks.Add(task);

                // If we've reached max concurrent uploads, wait for at least one to complete
                if (uploadTasks.Count >= MaxConcurrentUploads)
                {
                    var completedTask = await Task.WhenAny(uploadTasks).ConfigureAwait(false);
                    _ = uploadTasks.Remove(completedTask);
                    parts.Add(await completedTask.ConfigureAwait(false));
                }
            }

            // Wait for remaining uploads to complete
            foreach (var task in uploadTasks)
            {
                parts.Add(await task.ConfigureAwait(false));
            }

            // Sort parts by part number to ensure correct order
            var orderedParts = parts.OrderBy(p => p.PartNumber).ToList();

            // Complete multipart upload
            var completeRequest = new CompleteMultipartUploadRequest
            {
                BucketName = _bucket,
                Key = _key,
                UploadId = uploadId,
                PartETags = orderedParts,
            };

            if (_ifMatch is not null)
                completeRequest.IfMatch = _ifMatch;
            else if (!_overwrite)
                completeRequest.IfNoneMatch = "*";

            var completed = await _s3Client.CompleteMultipartUploadAsync(completeRequest, cancellationToken).ConfigureAwait(false);

            return completed?.ETag;
        }
        catch
        {
            // Abort multipart upload on failure
            try
            {
                var abortRequest = new AbortMultipartUploadRequest
                {
                    BucketName = _bucket,
                    Key = _key,
                    UploadId = uploadId,
                };

                _ = await _s3Client.AbortMultipartUploadAsync(abortRequest, CancellationToken.None).ConfigureAwait(false);
            }
            catch
            {
                // Ignore abort failures - the upload will eventually expire
            }

            throw;
        }
    }

    /// <summary>
    ///     Uploads a single part of a multipart upload.
    /// </summary>
    private async Task<PartETag> UploadPartAsync(
        int partNumber,
        long contentLength,
        string uploadId,
        SemaphoreSlim semaphore,
        CancellationToken cancellationToken)
    {
        await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            // Calculate offset and size for this part
            var offset = (long)(partNumber - 1) * _partSize;
            var partSize = (int)Math.Min(_partSize, contentLength - offset);

            // Allocate buffer per part to avoid race condition with parallel uploads
            var buffer = new byte[partSize];

            int bytesRead;

            lock (_readLock)
            {
                _content!.Position = offset;
                bytesRead = _content.Read(buffer, 0, partSize);
            }

            if (bytesRead != partSize)
                throw new IOException($"Expected to read {partSize} bytes for part {partNumber}, but only read {bytesRead} bytes.");

            // Upload the part
            using var partStream = new MemoryStream(buffer, 0, bytesRead, false);

            var uploadRequest = new UploadPartRequest
            {
                BucketName = _bucket,
                Key = _key,
                UploadId = uploadId,
                PartNumber = partNumber,
                PartSize = bytesRead,
                InputStream = partStream,
            };

            var response = await _s3Client.UploadPartAsync(uploadRequest, cancellationToken).ConfigureAwait(false);

            return new PartETag(partNumber, response.ETag);
        }
        finally
        {
            _ = semaphore.Release();
        }
    }
}
