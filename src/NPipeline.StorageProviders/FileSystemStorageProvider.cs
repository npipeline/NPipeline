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
                4096,
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
        var directory = Path.GetDirectoryName(path);

        if (!string.IsNullOrEmpty(directory))
            _ = Directory.CreateDirectory(directory);

        var stream = new FileStream(
            path,
            FileMode.Create,
            FileAccess.Write,
            FileShare.Read, // Allow other processes to read while writing
            4096,
            FileOptions.Asynchronous | FileOptions.SequentialScan);

        return Task.FromResult<StorageWriteStream>(new PassThroughWriteStream(stream));
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
                ETag = fileInfo.LastWriteTimeUtc.Ticks.ToString("x16"),
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

        // A manual walk, rather than SearchOption.AllDirectories, lets an inaccessible or vanished subtree be skipped
        // instead of aborting the listing. Reparse points (symlinks, junctions) are not followed, so the walk cannot loop.
        // There is deliberately no visited set keyed by path: on a case-sensitive file system a case-insensitive set
        // merged sibling directories such as "A" and "a".
        var pending = new Stack<string>();
        pending.Push(root);

        while (pending.Count > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();

            IEnumerator<FileSystemInfo> entries;

            try
            {
                // FileSystemInfo carries the attributes, length and timestamps from the directory read, with no further system calls per entry.
                entries = new DirectoryInfo(pending.Pop()).EnumerateFileSystemInfos("*", SearchOption.TopDirectoryOnly).GetEnumerator();
            }
            catch (Exception ex) when (ex is UnauthorizedAccessException or DirectoryNotFoundException)
            {
                continue;
            }

            using (entries)
            {
                while (true)
                {
                    FileSystemInfo entry;

                    try
                    {
                        if (!entries.MoveNext())
                            break;

                        entry = entries.Current;
                    }
                    catch (Exception ex) when (ex is UnauthorizedAccessException or DirectoryNotFoundException or FileNotFoundException)
                    {
                        break; // The directory changed or became unreadable mid-enumeration.
                    }

                    cancellationToken.ThrowIfCancellationRequested();

                    var isDirectory = (entry.Attributes & FileAttributes.Directory) != 0;

                    if (isDirectory && recursive)
                    {
                        if ((entry.Attributes & FileAttributes.ReparsePoint) == 0)
                            pending.Push(entry.FullName);

                        continue;
                    }

                    // WithPath keeps the caller's host and parameters on every listed URI.
                    var path = StorageUri.FromFilePath(entry.FullName).Path;

                    yield return new StorageItem
                    {
                        Uri = directory.WithPath(isDirectory ? path + "/" : path),
                        Size = isDirectory ? null : ((FileInfo)entry).Length,
                        LastModified = entry.LastWriteTimeUtc,
                        IsDirectory = isDirectory,
                    };
                }
            }
        }
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
            ".parquet" => "application/octet-stream",
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
