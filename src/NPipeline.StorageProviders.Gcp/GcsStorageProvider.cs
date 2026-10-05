using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using Google;
using Google.Apis.Storage.v1;
using Google.Apis.Storage.v1.Data;
using Google.Cloud.Storage.V1;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Gcp.Reliability;
using NPipeline.StorageProviders.Models;
using NResilience;

namespace NPipeline.StorageProviders.Gcp;

/// <summary>
///     Storage provider for Google Cloud Storage that derives from <see cref="StorageProvider" />.
///     Handles "gs" scheme URIs and supports reading, writing, listing, deleting, moving and metadata operations.
/// </summary>
/// <remarks>
///     <para>
///         - Async-first API design
///         - Stream-based I/O for scalability
///         - Proper error handling and exception translation
///         - Cancellation token support throughout
///         - Thread-safe implementation
///         - Consistent with existing S3/Azure provider patterns
///     </para>
///     <para>
///         URI format: gs://bucket-name/path/to/object
///         Supported URI parameters: projectId, contentType, serviceUrl. Credentials are never read from the URI; set
///         <see cref="GcsStorageProviderOptions.DefaultCredentials" />.
///     </para>
/// </remarks>
public sealed class GcsStorageProvider : StorageProvider
{
    private static readonly IReadOnlyList<StorageScheme> SupportedSchemes = [StorageScheme.Gcs];

    // The SDK's own retry of metadata calls is off, so NResilience is the only layer that retries them.
    private static readonly GetObjectOptions GetObjectOnce = new() { RetryOptions = RetryOptions.Never };

    private static readonly DeleteObjectOptions DeleteObjectOnce = new() { RetryOptions = RetryOptions.Never };

    private readonly GcsClientFactory _clientFactory;
    private readonly GcsStorageProviderOptions _options;
    private readonly Resilience _resilience;

    /// <summary>
    ///     Initializes a new instance of the <see cref="GcsStorageProvider" /> class.
    /// </summary>
    /// <param name="clientFactory">The GCS client factory.</param>
    /// <param name="options">The GCS storage provider options.</param>
    public GcsStorageProvider(GcsClientFactory clientFactory, GcsStorageProviderOptions options)
    {
        _clientFactory = clientFactory ?? throw new ArgumentNullException(nameof(clientFactory));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _resilience = _options.Resilience ?? throw new ArgumentException("Resilience must not be null.", nameof(options));
    }

    /// <inheritdoc />
    public override string Name => "Google Cloud Storage";

    /// <inheritdoc />
    public override IReadOnlyList<StorageScheme> Schemes => SupportedSchemes;

    /// <inheritdoc />
    /// <remarks>GCS is a flat object store, so it declares neither hierarchy nor atomic move: a move is a copy followed by a delete.</remarks>
    public override StorageCapabilities Capabilities =>
        StorageCapabilities.Read | StorageCapabilities.Write | StorageCapabilities.List | StorageCapabilities.Delete | StorageCapabilities.Move;

    /// <inheritdoc />
    /// <remarks>
    ///     The object streams from the HTTP response; nothing is downloaded before this method returns. A transient failure
    ///     mid-read reopens the object at the current offset, pinned to the first response's generation (see
    ///     <see cref="GcsResumingReadStream" />), under <see cref="GcsStorageProviderOptions.Resilience" />.
    /// </remarks>
    protected override async Task<Stream> OpenReadCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        var (bucket, objectName) = GetBucketAndObjectName(uri, true);
        var client = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);

        async Task<GcsResumingReadStream.Opened> OpenAsync(long position, long? generation, CancellationToken token)
        {
            try
            {
                return await RunAsync(t => OpenObjectAsync(client, bucket, objectName, position, generation, t), token).ConfigureAwait(false);
            }
            catch (GoogleApiException ex)
            {
                throw GcsErrors.Translate(ex, bucket, objectName, "read");
            }
        }

        var first = await OpenAsync(0, null, cancellationToken).ConfigureAwait(false);

        return new GcsResumingReadStream(first, OpenAsync, _resilience);
    }

    private static async Task<GcsResumingReadStream.Opened> OpenObjectAsync(
        StorageClient client,
        string bucket,
        string objectName,
        long position,
        long? generation,
        CancellationToken cancellationToken)
    {
        var request = client.Service.Objects.Get(bucket, objectName);
        request.Alt = ObjectsResource.GetRequest.AltEnum.Media;

        // A resume must read the same object version it started with, or the bytes would be spliced from two versions.
        request.IfGenerationMatch = generation;

        // Without gzip transfer encoding the response bytes are the object bytes, so a byte offset means the same thing
        // on every request.
        using var message = request.CreateRequest(false);

        if (position > 0)
            message.Headers.Range = new RangeHeaderValue(position, null);

        var response = await client.Service.HttpClient
            .SendAsync(message, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);

        try
        {
            if (!response.IsSuccessStatusCode)
            {
                var error = await client.Service.DeserializeError(response).ConfigureAwait(false);

                throw new GoogleApiException(client.Service.Name, error?.Message ?? response.ReasonPhrase)
                {
                    Error = error,
                    HttpStatusCode = response.StatusCode,
                };
            }

            // A server that ignores Range answers 200 with the whole object; splicing that in would corrupt the stream.
            if (position > 0 && response.StatusCode != HttpStatusCode.PartialContent)
                throw new IOException($"The GCS server did not honour the range request at offset {position}.");

            long? objectGeneration = null;

            if (response.Headers.TryGetValues("x-goog-generation", out var values) &&
                long.TryParse(values.FirstOrDefault(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
                objectGeneration = parsed;

            var length = response.Content.Headers.ContentEncoding.Count == 0
                ? response.Content.Headers.ContentLength
                : null;

            var content = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);

            return new GcsResumingReadStream.Opened(content, response, objectGeneration, length);
        }
        catch
        {
            response.Dispose();
            throw;
        }
    }

    /// <inheritdoc />
    protected override async Task<StorageWriteStream> OpenWriteCoreAsync(StorageUri uri, StorageWriteOptions? options, CancellationToken cancellationToken)
    {
        var (bucket, objectName) = GetBucketAndObjectName(uri, true);
        var client = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);

        // The write options win; the contentType URI parameter is the fallback.
        var contentType = !string.IsNullOrEmpty(options?.ContentType)
            ? options.ContentType
            : uri.Parameters.TryGetValue("contentType", out var ct) && !string.IsNullOrEmpty(ct)
                ? ct
                : null;

        return new GcsWriteStream(
            client,
            bucket,
            objectName,
            contentType,
            _options.UploadChunkSizeBytes);
    }

    /// <inheritdoc />
    protected override async Task<bool> ExistsCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        var (bucket, objectName) = GetBucketAndObjectName(uri, true);
        var client = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);

        try
        {
            _ = await RunAsync(
                token => client.GetObjectAsync(bucket, objectName, GetObjectOnce, token),
                cancellationToken).ConfigureAwait(false);

            return true;
        }
        catch (GoogleApiException ex) when (ex.HttpStatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }
        catch (GoogleApiException ex)
        {
            throw GcsErrors.Translate(ex, bucket, objectName, "exists");
        }
    }

    /// <inheritdoc />
    protected override IAsyncEnumerable<StorageItem> ListCoreAsync(StorageUri directory, bool recursive, CancellationToken cancellationToken) =>
        ListAsyncCore(directory, recursive, cancellationToken);

    /// <inheritdoc />
    protected override async Task<StorageMetadata?> GetMetadataCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        var (bucket, objectName) = GetBucketAndObjectName(uri, true);
        var client = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);

        try
        {
            var obj = await RunAsync(
                token => client.GetObjectAsync(bucket, objectName, GetObjectOnce, token),
                cancellationToken).ConfigureAwait(false);

            var customMetadata = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            // Add GCS object metadata
            if (obj.Metadata is not null)
            {
                foreach (var kvp in obj.Metadata)
                {
                    customMetadata[kvp.Key] = kvp.Value;
                }
            }

            return new StorageMetadata
            {
                Size = (long)(obj.Size ?? 0),
                LastModified = obj.UpdatedDateTimeOffset,
                ContentType = obj.ContentType,
                ETag = obj.ETag,
                CustomMetadata = customMetadata,
                IsDirectory = false,
            };
        }
        catch (GoogleApiException ex) when (ex.HttpStatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
        catch (GoogleApiException ex)
        {
            throw GcsErrors.Translate(ex, bucket, objectName, "metadata");
        }
    }

    /// <inheritdoc />
    protected override async Task DeleteCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        var (bucket, objectName) = GetBucketAndObjectName(uri, true);
        var client = await _clientFactory.GetClientAsync(uri, cancellationToken).ConfigureAwait(false);

        try
        {
            await DeleteObjectAsync(client, bucket, objectName, cancellationToken).ConfigureAwait(false);
        }
        catch (GoogleApiException ex) when (ex.HttpStatusCode == HttpStatusCode.NotFound)
        {
            // Deleting a missing object succeeds.
        }
        catch (GoogleApiException ex)
        {
            throw GcsErrors.Translate(ex, bucket, objectName, "delete");
        }
    }

    /// <inheritdoc />
    /// <remarks>
    ///     GCS has no rename. The object is copied with a server-side rewrite and then the source is deleted, so a failure
    ///     between the two steps leaves both objects. The destination is overwritten.
    /// </remarks>
    protected override async Task MoveCoreAsync(StorageUri source, StorageUri destination, CancellationToken cancellationToken)
    {
        var (sourceBucket, sourceName) = GetBucketAndObjectName(source, true);
        var (destinationBucket, destinationName) = GetBucketAndObjectName(destination, true);
        var client = await _clientFactory.GetClientAsync(source, cancellationToken).ConfigureAwait(false);

        try
        {
            // Copying an object onto itself and then deleting the source would lose it.
            if (string.Equals(sourceBucket, destinationBucket, StringComparison.Ordinal)
                && string.Equals(sourceName, destinationName, StringComparison.Ordinal))
            {
                _ = await RunAsync(
                    token => client.GetObjectAsync(sourceBucket, sourceName, GetObjectOnce, token),
                    cancellationToken).ConfigureAwait(false);

                return;
            }

            _ = await RunAsync(
                token => client.CopyObjectAsync(sourceBucket, sourceName, destinationBucket, destinationName, cancellationToken: token),
                cancellationToken).ConfigureAwait(false);

            try
            {
                await DeleteObjectAsync(client, sourceBucket, sourceName, cancellationToken).ConfigureAwait(false);
            }
            catch (GoogleApiException ex) when (ex.HttpStatusCode == HttpStatusCode.NotFound)
            {
                // A retried delete can find that its first attempt already removed the source.
            }
        }
        catch (GoogleApiException ex)
        {
            throw GcsErrors.Translate(ex, sourceBucket, sourceName, "move");
        }
    }

    private async Task DeleteObjectAsync(StorageClient client, string bucket, string objectName, CancellationToken cancellationToken) =>
        _ = await RunAsync(
            async token =>
            {
                await client.DeleteObjectAsync(bucket, objectName, DeleteObjectOnce, token).ConfigureAwait(false);
                return true;
            },
            cancellationToken).ConfigureAwait(false);


    private static (string bucket, string objectName) GetBucketAndObjectName(StorageUri uri, bool requireObjectName)
    {
        var bucket = uri.Host;

        if (string.IsNullOrEmpty(bucket))
            throw new ArgumentException("GCS URI must specify a bucket name in the host component.", nameof(uri));

        var objectName = uri.Path.TrimStart('/');

        if (requireObjectName && string.IsNullOrWhiteSpace(objectName))
            throw new ArgumentException("GCS URI must specify an object name in the path component.", nameof(uri));

        return (bucket, objectName);
    }

    private async IAsyncEnumerable<StorageItem> ListAsyncCore(
        StorageUri prefix,
        bool recursive,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var (bucket, prefixPath) = GetBucketAndObjectName(prefix, false);
        var client = await _clientFactory.GetClientAsync(prefix, cancellationToken).ConfigureAwait(false);
        var request = client.Service.Objects.List(bucket);
        request.Prefix = prefixPath;

        // Only the fields this method reads; it shrinks every page.
        request.Fields = "items(name,size,updated),prefixes,nextPageToken";

        if (!recursive)
            request.Delimiter = "/";

        string? pageToken = null;

        do
        {
            cancellationToken.ThrowIfCancellationRequested();
            request.PageToken = pageToken;

            Objects response;

            try
            {
                response = await RunAsync(
                    token => request.ExecuteAsync(token),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (GoogleApiException ex) when (ex.HttpStatusCode == HttpStatusCode.NotFound)
            {
                yield break;
            }
            catch (GoogleApiException ex)
            {
                throw GcsErrors.Translate(ex, bucket, prefixPath, "list");
            }

            foreach (var obj in response.Items ?? [])
            {
                cancellationToken.ThrowIfCancellationRequested();

                // Names ending in '/' are folder placeholder objects, not files.
                if (obj is null || string.IsNullOrWhiteSpace(obj.Name) || obj.Name.EndsWith('/'))
                    continue;

                var itemUri = prefix.WithPath("/" + obj.Name);

                yield return new StorageItem
                {
                    Uri = itemUri,
                    Size = obj.Size is { } size ? (long)size : null,
                    LastModified = obj.UpdatedDateTimeOffset,
                    IsDirectory = false,
                };
            }

            if (!recursive)
            {
                foreach (var commonPrefix in response.Prefixes ?? [])
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (string.IsNullOrWhiteSpace(commonPrefix))
                        continue;

                    yield return new StorageItem
                    {
                        Uri = prefix.WithPath("/" + commonPrefix),
                        IsDirectory = true,
                    };
                }
            }

            pageToken = response.NextPageToken;
        } while (!string.IsNullOrWhiteSpace(pageToken));
    }

    private ValueTask<T> RunAsync<T>(Func<CancellationToken, Task<T>> operation, CancellationToken cancellationToken)
    {
        return _resilience.RunAsync(
            static (operation, token) => GcsRetryAfter.CaptureAsync(operation, token),
            operation,
            cancellationToken);
    }
}
