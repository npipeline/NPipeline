using System.IO.Enumeration;
using System.Runtime.CompilerServices;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders;

/// <summary>
///     Built-in storage provider for the local file system. Serves <c>file://</c> URIs and the local paths that
///     <see cref="StorageUri.Parse" /> converts into them.
/// </summary>
public sealed class FileSystemStorageProvider : StorageProvider
{
    private static readonly IReadOnlyList<StorageScheme> SupportedSchemes = [StorageScheme.File];

    /// <inheritdoc />
    public override string Name => "File System";

    /// <inheritdoc />
    public override IReadOnlyList<StorageScheme> Schemes => SupportedSchemes;

    /// <inheritdoc />
    public override StorageCapabilities Capabilities =>
        StorageCapabilities.Read | StorageCapabilities.Write | StorageCapabilities.List | StorageCapabilities.Delete
        | StorageCapabilities.Move | StorageCapabilities.AtomicMove | StorageCapabilities.Hierarchy;

    /// <inheritdoc />
    protected override Task<Stream> OpenReadCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        var path = ToLocalPath(uri);

        try
        {
            // An explicit FileStream sets useAsync; File.OpenRead does not.
            Stream stream = new FileStream(
                path,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                0, // The connectors buffer through FileNodeOptions.BufferSize; a second buffer here only adds a copy.
                FileOptions.Asynchronous | FileOptions.SequentialScan);

            return Task.FromResult(stream);
        }
        catch (DirectoryNotFoundException ex)
        {
            throw new FileNotFoundException($"Could not find file '{uri}'.", path, ex);
        }
    }

    /// <inheritdoc />
    protected override Task<StorageWriteStream> OpenWriteCoreAsync(StorageUri uri, StorageWriteOptions? options, CancellationToken cancellationToken)
    {
        var path = ToLocalPath(uri);

        return Task.FromResult<StorageWriteStream>(new FileSystemWriteStream(path, options?.Overwrite ?? true));
    }

    /// <inheritdoc />
    protected override Task<StorageMetadata?> GetMetadataCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        var path = ToLocalPath(uri);

        if (File.Exists(path))
        {
            var fileInfo = new FileInfo(path);

            return Task.FromResult<StorageMetadata?>(new StorageMetadata
            {
                Size = fileInfo.Length,
                LastModified = fileInfo.LastWriteTimeUtc,
                ContentType = GetContentType(path),
                ETag = $"{fileInfo.LastWriteTimeUtc.Ticks:x}-{fileInfo.Length:x}",
            });
        }

        if (Directory.Exists(path))
        {
            var dirInfo = new DirectoryInfo(path);

            return Task.FromResult<StorageMetadata?>(new StorageMetadata
            {
                Size = 0,
                LastModified = dirInfo.LastWriteTimeUtc,
                IsDirectory = true,
                ETag = dirInfo.LastWriteTimeUtc.Ticks.ToString("x16"),
            });
        }

        return Task.FromResult<StorageMetadata?>(null);
    }

    /// <inheritdoc />
    protected override Task<bool> ExistsCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        var path = ToLocalPath(uri);
        return Task.FromResult(File.Exists(path) || Directory.Exists(path));
    }

    /// <inheritdoc />
    protected override IAsyncEnumerable<StorageItem> ListCoreAsync(StorageUri directory, bool recursive, CancellationToken cancellationToken) =>
        ListIteratorAsync(directory, ToLocalPath(directory), recursive, cancellationToken);

    /// <inheritdoc />
    protected override Task DeleteCoreAsync(StorageUri uri, CancellationToken cancellationToken)
    {
        // File.Delete is idempotent: it does not throw when the file is missing.
        File.Delete(ToLocalPath(uri));
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    protected override Task MoveCoreAsync(StorageUri source, StorageUri destination, CancellationToken cancellationToken)
    {
        var sourcePath = ToLocalPath(source);
        var destinationPath = ToLocalPath(destination);

        if (!File.Exists(sourcePath))
            throw new FileNotFoundException($"Could not find file '{source}'.", sourcePath);

        var directory = Path.GetDirectoryName(destinationPath);

        if (!string.IsNullOrEmpty(directory))
            _ = Directory.CreateDirectory(directory);

        File.Move(sourcePath, destinationPath, true);
        return Task.CompletedTask;
    }

    private static async IAsyncEnumerable<StorageItem> ListIteratorAsync(
        StorageUri directory,
        string root,
        bool recursive,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Await once so this is a valid async iterator without a per-item cost.
        await Task.CompletedTask.ConfigureAwait(false);

        // One enumeration reads each entry's length, timestamp and attributes from the directory read itself, so listing
        // costs no further system call per entry. Reparse points (symlinks, junctions) are not followed, so the walk cannot
        // loop, and an inaccessible subtree is skipped instead of aborting the listing. A recursive listing yields files only.
        var options = new EnumerationOptions
        {
            RecurseSubdirectories = recursive,
            IgnoreInaccessible = true,
            AttributesToSkip = 0,
            ReturnSpecialDirectories = false,
        };

        IEnumerator<StorageItem> enumerator;

        try
        {
            // Constructing the enumerable opens the directory.
            var entries = new FileSystemEnumerable<StorageItem>(root, (ref FileSystemEntry entry) => ToItem(directory, ref entry), options)
            {
                ShouldRecursePredicate = static (ref FileSystemEntry entry) => (entry.Attributes & FileAttributes.ReparsePoint) == 0,
                ShouldIncludePredicate = recursive
                    ? static (ref FileSystemEntry entry) => !entry.IsDirectory
                    : null,
            };

            enumerator = entries.GetEnumerator();
        }
        catch (Exception ex) when (ex is DirectoryNotFoundException or FileNotFoundException)
        {
            yield break;
        }

        using var _ = enumerator;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                if (!enumerator.MoveNext())
                    yield break;
            }
            catch (Exception ex) when (ex is DirectoryNotFoundException or FileNotFoundException)
            {
                yield break; // The directory is missing, or was deleted during the listing.
            }

            yield return enumerator.Current;
        }
    }

    private static StorageItem ToItem(StorageUri directory, ref FileSystemEntry entry)
    {
        // The full path is already known; building the URI from it needs no further path resolution. WithPath keeps the
        // caller's host and parameters on every listed URI.
        var path = entry.ToFullPath();

        if (Path.DirectorySeparatorChar == '\\')
        {
            path = path.Replace('\\', '/');

            // \\server\share\x is the UNC path of host "server", which the URI holds in Host.
            if (path.StartsWith("//", StringComparison.Ordinal))
            {
                var hostEnd = path.IndexOf('/', 2);
                path = hostEnd < 0 ? "/" : path[hostEnd..];
            }
        }

        var isDirectory = entry.IsDirectory;

        return new StorageItem
        {
            Uri = directory.WithPath(isDirectory ? path + "/" : path),
            Size = isDirectory ? null : entry.Length,
            LastModified = entry.LastWriteTimeUtc,
            IsDirectory = isDirectory,
        };
    }

    private static string GetContentType(string filePath)
    {
        var ext = Path.GetExtension(filePath).ToLowerInvariant();

        return ext switch
        {
            ".csv" => "text/csv",
            ".json" => "application/json",
            ".xml" => "application/xml",
            ".txt" => "text/plain",
            ".xlsx" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ".xls" => "application/vnd.ms-excel",
            ".zip" => "application/zip",
            ".pdf" => "application/pdf",
            ".parquet" => "application/vnd.apache.parquet",
            _ => "application/octet-stream",
        };
    }

    private static string ToLocalPath(StorageUri uri)
    {
        // "file://localhost/path" is the RFC 8089 spelling of a local path. Any other host names a UNC share
        // (\\host\share\path), which exists only on Windows.
        if (!string.IsNullOrWhiteSpace(uri.Host) && !string.Equals(uri.Host, "localhost", StringComparison.OrdinalIgnoreCase))
        {
            if (!OperatingSystem.IsWindows())
                throw new NotSupportedException($"UNC paths (file://{uri.Host}/...) are only supported on Windows.");

            return $"\\\\{uri.Host}{uri.Path.Replace('/', '\\')}";
        }

        // Local drive: "/C:/folder/file" -> "C:\folder\file" (Windows only)
        if (uri.Path.Length >= 3
            && uri.Path[0] == '/'
            && char.IsLetter(uri.Path[1])
            && uri.Path[2] == ':')
        {
            var win = uri.Path[1..].Replace('/', '\\');
            return win;
        }

        // On Unix-like systems (macOS, Linux), paths use forward slashes and should not be converted
        // On Windows, paths use backslashes and should be converted
        if (Path.DirectorySeparatorChar == '\\')
        {
            // Windows: convert forward slashes to backslashes
            return uri.Path.Replace('/', '\\');
        }

        // Unix-like (macOS, Linux): use forward slashes as-is
        return uri.Path;
    }
}
