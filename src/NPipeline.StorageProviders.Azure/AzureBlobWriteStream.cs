using Azure;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.StorageProviders.Azure;

/// <summary>
///     A write stream that buffers to a local file and uploads the blob in <see cref="StorageWriteStream.CommitAsync" />.
///     Disposing without committing uploads nothing.
/// </summary>
public sealed class AzureBlobWriteStream : SpooledWriteStream
{
    private readonly string _blob;
    private readonly BlobServiceClient _blobServiceClient;
    private readonly long _blockBlobUploadThreshold;
    private readonly string _container;
    private readonly string? _contentType;
    private readonly int? _maximumConcurrency;
    private readonly string? _ifMatch;
    private readonly int? _maximumTransferSizeBytes;
    private readonly bool _overwrite;

    /// <summary>
    ///     Initializes a new instance of the <see cref="AzureBlobWriteStream" /> class.
    /// </summary>
    /// <param name="blobServiceClient">The Azure Blob Service client.</param>
    /// <param name="container">The Azure container name.</param>
    /// <param name="blob">The Azure blob name.</param>
    /// <param name="contentType">Optional content type for the upload.</param>
    /// <param name="blockBlobUploadThreshold">Threshold in bytes for applying the transfer options.</param>
    /// <param name="maximumConcurrency">Maximum concurrent upload requests for large blobs.</param>
    /// <param name="maximumTransferSizeBytes">Maximum transfer size in bytes for each upload chunk.</param>
    /// <param name="ifMatch">Commit only if the blob's current ETag matches.</param>
    /// <param name="overwrite">When <see langword="false" />, commit fails if the blob exists.</param>
    public AzureBlobWriteStream(
        BlobServiceClient blobServiceClient,
        string container,
        string blob,
        string? contentType = null,
        long blockBlobUploadThreshold = 64 * 1024 * 1024,
        int? maximumConcurrency = null,
        int? maximumTransferSizeBytes = null,
        string? ifMatch = null,
        bool overwrite = true)
        : base("azure-upload")
    {
        _blobServiceClient = blobServiceClient ?? throw new ArgumentNullException(nameof(blobServiceClient));
        _container = container ?? throw new ArgumentNullException(nameof(container));
        _blob = blob ?? throw new ArgumentNullException(nameof(blob));
        _contentType = contentType;
        _blockBlobUploadThreshold = blockBlobUploadThreshold;
        _maximumConcurrency = maximumConcurrency;
        _maximumTransferSizeBytes = maximumTransferSizeBytes;
        _ifMatch = ifMatch;
        _overwrite = overwrite;
    }

    /// <inheritdoc />
    protected override async Task<string?> UploadAsync(Stream content, CancellationToken cancellationToken)
    {
        var containerClient = _blobServiceClient.GetBlobContainerClient(_container);
        var blobClient = containerClient.GetBlobClient(_blob);

        var options = new BlobUploadOptions();

        if (!string.IsNullOrEmpty(_contentType))
            options.HttpHeaders = new BlobHttpHeaders { ContentType = _contentType };

        if (content.Length >= _blockBlobUploadThreshold && (_maximumConcurrency.HasValue || _maximumTransferSizeBytes.HasValue))
        {
            var transferOptions = new StorageTransferOptions();

            if (_maximumConcurrency.HasValue)
                transferOptions.MaximumConcurrency = _maximumConcurrency.Value;

            if (_maximumTransferSizeBytes.HasValue)
                transferOptions.MaximumTransferSize = _maximumTransferSizeBytes.Value;

            options.TransferOptions = transferOptions;
        }

        if (_ifMatch is not null)
            options.Conditions = new BlobRequestConditions { IfMatch = new global::Azure.ETag(_ifMatch) };
        else if (!_overwrite)
            options.Conditions = new BlobRequestConditions { IfNoneMatch = global::Azure.ETag.All };

        try
        {
            // Ensure container exists before uploading
            _ = await containerClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

            var response = await blobClient.UploadAsync(content, options, cancellationToken).ConfigureAwait(false);
            var etag = response?.Value?.ETag.ToString();

            return string.IsNullOrEmpty(etag) ? null : etag;
        }
        catch (RequestFailedException ex)
        {
            throw AzureErrors.Translate(ex, _container, _blob);
        }
    }
}
