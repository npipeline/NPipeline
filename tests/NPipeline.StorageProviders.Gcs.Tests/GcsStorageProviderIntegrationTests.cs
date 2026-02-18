using System.Text;
using NPipeline.StorageProviders.Models;
using Xunit;

namespace NPipeline.StorageProviders.Gcs.Tests;

/// <summary>
///     Integration tests for <see cref="GcsStorageProvider" />.
///     These tests run against a real GCS endpoint or emulator when explicitly enabled.
/// </summary>
public sealed class GcsStorageProviderIntegrationTests
{
    private static readonly string RunId = $"it-{DateTime.UtcNow:yyyyMMddHHmmss}-{Guid.NewGuid():N}";

    [Fact]
    public async Task OpenWriteAndOpenRead_RoundTripContent()
    {
        if (!TryCreateContext(out var context))
            return;

        var objectPath = $"{RunId}/roundtrip.txt";
        var uri = StorageUri.Parse($"gs://{context.Bucket}/{objectPath}");
        var expected = $"hello-gcs-{Guid.NewGuid():N}";

        await using (var writeStream = await context.Provider.OpenWriteAsync(uri))
        await using (var writer = new StreamWriter(writeStream, Encoding.UTF8, leaveOpen: false))
        {
            await writer.WriteAsync(expected);
        }

        await using var readStream = await context.Provider.OpenReadAsync(uri);
        using var reader = new StreamReader(readStream, Encoding.UTF8);
        var actual = await reader.ReadToEndAsync();

        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task ExistsAndMetadata_ReturnExpectedValues()
    {
        if (!TryCreateContext(out var context))
            return;

        var objectPath = $"{RunId}/metadata.txt";
        var uri = StorageUri.Parse($"gs://{context.Bucket}/{objectPath}?contentType=text/plain");

        await using (var writeStream = await context.Provider.OpenWriteAsync(uri))
        {
            var payload = Encoding.UTF8.GetBytes("metadata payload");
            await writeStream.WriteAsync(payload, CancellationToken.None);
        }

        var exists = await context.Provider.ExistsAsync(uri);
        Assert.True(exists);

        var metadata = await context.Provider.GetMetadataAsync(uri);
        Assert.NotNull(metadata);
        Assert.True(metadata!.Size > 0);
        Assert.Equal("text/plain", metadata.ContentType);
        Assert.False(metadata.IsDirectory);
    }

    [Fact]
    public async Task ListAsync_RecursiveAndNonRecursive_ReturnsExpectedItems()
    {
        if (!TryCreateContext(out var context))
            return;

        var basePrefix = $"{RunId}/list";

        var object1 = StorageUri.Parse($"gs://{context.Bucket}/{basePrefix}/a/file1.txt");
        var object2 = StorageUri.Parse($"gs://{context.Bucket}/{basePrefix}/b/file2.txt");
        var prefixUri = StorageUri.Parse($"gs://{context.Bucket}/{basePrefix}/");

        await WriteContentAsync(context.Provider, object1, "one");
        await WriteContentAsync(context.Provider, object2, "two");

        var recursiveItems = new List<StorageItem>();
        await foreach (var item in context.Provider.ListAsync(prefixUri, recursive: true))
            recursiveItems.Add(item);

        Assert.Contains(recursiveItems, item => item.Uri.Path.EndsWith("/a/file1.txt", StringComparison.Ordinal));
        Assert.Contains(recursiveItems, item => item.Uri.Path.EndsWith("/b/file2.txt", StringComparison.Ordinal));

        var nonRecursiveItems = new List<StorageItem>();
        await foreach (var item in context.Provider.ListAsync(prefixUri, recursive: false))
            nonRecursiveItems.Add(item);

        Assert.Contains(nonRecursiveItems, item => item.IsDirectory && item.Uri.Path.EndsWith("/a", StringComparison.Ordinal));
        Assert.Contains(nonRecursiveItems, item => item.IsDirectory && item.Uri.Path.EndsWith("/b", StringComparison.Ordinal));
    }

    private static async Task WriteContentAsync(GcsStorageProvider provider, StorageUri uri, string content)
    {
        await using var writeStream = await provider.OpenWriteAsync(uri);
        await using var writer = new StreamWriter(writeStream, Encoding.UTF8, leaveOpen: false);
        await writer.WriteAsync(content);
    }

    private static bool TryCreateContext(out IntegrationContext context)
    {
        context = null!;

        var enabled = Environment.GetEnvironmentVariable("NP_GCS_INTEGRATION");
        if (!string.Equals(enabled, "1", StringComparison.OrdinalIgnoreCase) &&
            !string.Equals(enabled, "true", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var bucket = Environment.GetEnvironmentVariable("NP_GCS_BUCKET");
        if (string.IsNullOrWhiteSpace(bucket))
            return false;

        var options = new GcsStorageProviderOptions
        {
            DefaultProjectId = Environment.GetEnvironmentVariable("NP_GCS_PROJECT_ID") ?? "test-project",
            UseDefaultCredentials = true,
        };

        var serviceUrl = Environment.GetEnvironmentVariable("NP_GCS_SERVICE_URL");
        if (!string.IsNullOrWhiteSpace(serviceUrl) && Uri.TryCreate(serviceUrl, UriKind.Absolute, out var endpoint))
            options.ServiceUrl = endpoint;

        var clientFactory = new GcsClientFactory(options);
        var provider = new GcsStorageProvider(clientFactory, options);

        context = new IntegrationContext(provider, bucket.Trim());
        return true;
    }

    private sealed record IntegrationContext(GcsStorageProvider Provider, string Bucket);
}
