using Azure;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.StorageProviders.Adls;

/// <summary>
///     A write stream that buffers to a local file and uploads the file in <see cref="StorageWriteStream.CommitAsync" />.
///     Disposing without committing uploads nothing.
/// </summary>
public sealed class AdlsGen2WriteStream : SpooledWriteStream
{
    private readonly BlobServiceClient _blobServiceClient;
    private readonly string? _contentType;
    private readonly string _filesystem;
    private readonly int? _maximumConcurrency;
    private readonly string? _ifMatch;
    private readonly int? _maximumTransferSizeBytes;
    private readonly bool _overwrite;
    private readonly string _path;
    private readonly long _uploadThreshold;

    /// <summary>
    ///     Initializes a new instance of the <see cref="AdlsGen2WriteStream" /> class.
    /// </summary>
    /// <param name="blobServiceClient">
    ///     The Azure Blob Service client (used for uploads via the Blob API, which is compatible with Azurite and all ADLS Gen2
    ///     configurations).
    /// </param>
    /// <param name="filesystem">The ADLS filesystem name (maps to a Blob container).</param>
    /// <param name="path">The ADLS path.</param>
    /// <param name="contentType">Optional content type for the upload.</param>
    /// <param name="uploadThreshold">Threshold in bytes for applying the transfer options.</param>
    /// <param name="maximumConcurrency">Maximum concurrent upload requests for large files.</param>
    /// <param name="maximumTransferSizeBytes">Maximum transfer size in bytes for each upload chunk.</param>
    /// <param name="ifMatch">Commit only if the blob's current ETag matches.</param>
    /// <param name="overwrite">When <see langword="false" />, commit fails if the blob exists.</param>
    public AdlsGen2WriteStream(
        BlobServiceClient blobServiceClient,
        string filesystem,
        string path,
        string? contentType = null,
        long uploadThreshold = 64 * 1024 * 1024,
        int? maximumConcurrency = null,
        int? maximumTransferSizeBytes = null,
        string? ifMatch = null,
        bool overwrite = true)
        : base("adls-upload")
    {
        _blobServiceClient = blobServiceClient ?? throw new ArgumentNullException(nameof(blobServiceClient));
        _filesystem = filesystem ?? throw new ArgumentNullException(nameof(filesystem));
        _path = path ?? throw new ArgumentNullException(nameof(path));
        _contentType = contentType;
        _uploadThreshold = uploadThreshold;
        _maximumConcurrency = maximumConcurrency;
        _maximumTransferSizeBytes = maximumTransferSizeBytes;
        _ifMatch = ifMatch;
        _overwrite = overwrite;
    }

    /// <inheritdoc />
    protected override async Task<string?> UploadAsync(Stream content, CancellationToken cancellationToken)
    {
        var containerClient = _blobServiceClient.GetBlobContainerClient(_filesystem);
        var blobClient = containerClient.GetBlobClient(_path);

        var options = new BlobUploadOptions();

        if (!string.IsNullOrEmpty(_contentType))
            options.HttpHeaders = new BlobHttpHeaders { ContentType = _contentType };

        if (content.Length >= _uploadThreshold && (_maximumConcurrency.HasValue || _maximumTransferSizeBytes.HasValue))
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
            _ = await containerClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

            var response = await blobClient.UploadAsync(content, options, cancellationToken).ConfigureAwait(false);
            var etag = response?.Value?.ETag.ToString();

            return string.IsNullOrEmpty(etag) ? null : etag;
        }
        catch (RequestFailedException ex)
        {
            throw AdlsErrors.Translate(ex, _filesystem, _path);
        }
    }
}
