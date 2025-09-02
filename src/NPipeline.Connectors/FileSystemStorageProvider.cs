using System.Runtime.CompilerServices;
using NPipeline.Connectors.Abstractions;

namespace NPipeline.Connectors;

/// <summary>
///     Built-in storage provider for local file system access.
///     Handles "file" scheme URIs and local paths converted via <see cref="StorageUri" />.
/// </summary>
/// <remarks>
///     - Dependency-free implementation
///     - Stream-based operations for scalability
///     - Proper directory creation for write operations
///     - Conservative file sharing (read: FileShare.Read; write: FileShare.None)
/// </remarks>
public sealed class FileSystemStorageProvider : IStorageProvider, IStorageProviderMetadataProvider
{
    public StorageScheme Scheme => StorageScheme.File;

    public bool CanHandle(StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);
        return Scheme.Equals(uri.Scheme);
    }

    public Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var path = ToLocalPath(uri);

        // File.OpenRead uses FileShare.Read by default; use explicit FileStream to set useAsync true
        var stream = new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            4096,
            FileOptions.Asynchronous | FileOptions.SequentialScan);

        return Task.FromResult<Stream>(stream);
    }

    public Task<Stream> OpenWriteAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var path = ToLocalPath(uri);

        var directory = Path.GetDirectoryName(path);

        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
            Directory.CreateDirectory(directory);

        var stream = new FileStream(
            path,
            FileMode.Create, // overwrite by default; future enhancements may make this configurable
            FileAccess.Write,
            FileShare.None,
            4096,
            FileOptions.Asynchronous | FileOptions.SequentialScan);

        return Task.FromResult<Stream>(stream);
    }

    public Task<bool> ExistsAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var path = ToLocalPath(uri);
        var exists = File.Exists(path) || Directory.Exists(path);
        return Task.FromResult(exists);
    }

    public Task DeleteAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var path = ToLocalPath(uri);

        if (File.Exists(path))
            File.Delete(path);
        else if (Directory.Exists(path))
            Directory.Delete(path, true);

        return Task.CompletedTask;
    }

    public IAsyncEnumerable<StorageItem> ListAsync(
        StorageUri prefix,
        bool recursive = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        return ListAsyncCore(prefix, recursive, cancellationToken);
    }

    public Task<StorageMetadata?> GetMetadataAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var path = ToLocalPath(uri);

        if (File.Exists(path))
        {
            var fileInfo = new FileInfo(path);
            var contentType = GetContentType(path);

            var metadata = new StorageMetadata
            {
                Size = fileInfo.Length,
                LastModified = fileInfo.LastWriteTimeUtc,
                ContentType = contentType,
                CustomMetadata = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase),
                IsDirectory = false,
                ETag = fileInfo.LastWriteTimeUtc.Ticks.ToString("x16"),
            };

            return Task.FromResult<StorageMetadata?>(metadata);
        }

        if (Directory.Exists(path))
        {
            var dirInfo = new DirectoryInfo(path);

            var metadata = new StorageMetadata
            {
                Size = 0,
                LastModified = dirInfo.LastWriteTimeUtc,
                ContentType = null,
                CustomMetadata = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase),
                IsDirectory = true,
                ETag = dirInfo.LastWriteTimeUtc.Ticks.ToString("x16"),
            };

            return Task.FromResult<StorageMetadata?>(metadata);
        }

        return Task.FromResult<StorageMetadata?>(null);
    }

    public StorageProviderMetadata GetMetadata()
    {
        return new StorageProviderMetadata
        {
            Name = "File System",
            SupportedSchemes = [StorageScheme.File.ToString()],
            SupportsRead = true,
            SupportsWrite = true,
            SupportsDelete = true,
            SupportsListing = true,
            SupportsMetadata = true,
            SupportsHierarchy = true,
            Capabilities = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase),
        };
    }

    private static IAsyncEnumerable<StorageItem> ListAsyncCore(
        StorageUri prefix,
        bool recursive,
        CancellationToken cancellationToken)
    {
        var path = ToLocalPath(prefix);

        if (!Directory.Exists(path))
            return EmptyAsyncEnumerable();

        return ListAsyncCoreIterator(path, recursive, cancellationToken);
    }

    private static async IAsyncEnumerable<StorageItem> EmptyAsyncEnumerable()
    {
        await Task.CompletedTask;
        yield break;
    }

    private static async IAsyncEnumerable<StorageItem> ListAsyncCoreIterator(
        string path,
        bool recursive,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Await once before loop to satisfy async iterator requirement without per-item overhead
        await Task.CompletedTask;

        // For recursive listing, use manual stack-based traversal instead of AllDirectories.
        // This allows us to catch UnauthorizedAccessException at directory enumeration boundaries
        // and skip inaccessible subtrees gracefully instead of aborting entire enumeration.
        // Also prevents infinite loops with symlinks by tracking processed paths.
        if (recursive)
        {
            var directoriesToProcess = new Stack<string>();
            var processedPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            directoriesToProcess.Push(path);
            processedPaths.Add(path);

            while (directoriesToProcess.Count > 0)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var currentDir = directoriesToProcess.Pop();

                // Enumerate only direct children (TopDirectoryOnly) to catch exceptions at boundaries
                IEnumerable<string>? entries = null;

                try
                {
                    entries = Directory.EnumerateFileSystemEntries(currentDir, "*", SearchOption.TopDirectoryOnly);
                }
                catch (UnauthorizedAccessException)
                {
                    // Cannot access this directory, skip it and continue with remainder
                    continue;
                }
                catch (DirectoryNotFoundException)
                {
                    // Directory was deleted, skip it and continue with remainder
                    continue;
                }

                foreach (var entry in entries)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var itemUri = StorageUri.FromFilePath(entry);

                    // Between enumeration and attribute lookup, files can be deleted or moved.
                    FileAttributes attributes;

                    try
                    {
                        attributes = File.GetAttributes(entry);
                    }
                    catch (FileNotFoundException)
                    {
                        continue; // File was deleted after enumeration, skip
                    }
                    catch (DirectoryNotFoundException)
                    {
                        continue; // Directory was deleted after enumeration, skip
                    }
                    catch (UnauthorizedAccessException)
                    {
                        continue; // No permission to access this entry, skip
                    }

                    var isDir = (attributes & FileAttributes.Directory) != 0;

                    long size = 0;
                    var lastModified = DateTimeOffset.UtcNow;

                    try
                    {
                        if (isDir)
                        {
                            var dirInfo = new DirectoryInfo(entry);
                            lastModified = dirInfo.LastWriteTimeUtc;
                        }
                        else
                        {
                            var fileInfo = new FileInfo(entry);
                            size = fileInfo.Length;
                            lastModified = fileInfo.LastWriteTimeUtc;
                        }
                    }
                    catch (FileNotFoundException)
                    {
                        continue; // File/directory deleted, skip
                    }
                    catch (DirectoryNotFoundException)
                    {
                        continue; // Parent directory deleted, skip
                    }
                    catch (UnauthorizedAccessException)
                    {
                        continue; // Cannot read properties, skip
                    }

                    yield return new StorageItem
                    {
                        Uri = itemUri,
                        Size = size,
                        LastModified = lastModified,
                        IsDirectory = isDir,
                    };

                    // If this is a directory and we haven't processed it yet, queue it for traversal.
                    // However, skip reparse points (symlinks, junctions) to prevent infinite loops
                    // from circular references (e.g., junction pointing to ancestor).
                    var isReparsePoint = (attributes & FileAttributes.ReparsePoint) != 0;

                    if (isDir && !isReparsePoint && !processedPaths.Contains(entry))
                    {
                        processedPaths.Add(entry);
                        directoriesToProcess.Push(entry);
                    }
                }
            }
        }
        else
        {
            // Non-recursive: just enumerate direct children
            IEnumerable<string>? entries = null;

            try
            {
                entries = Directory.EnumerateFileSystemEntries(path, "*", SearchOption.TopDirectoryOnly);
            }
            catch (UnauthorizedAccessException)
            {
                // Cannot list at all (no permission on root)
                yield break;
            }
            catch (DirectoryNotFoundException)
            {
                // Root directory doesn't exist
                yield break;
            }

            foreach (var entry in entries)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var itemUri = StorageUri.FromFilePath(entry);

                // Between enumeration and attribute lookup, files can be deleted or moved.
                FileAttributes attributes;

                try
                {
                    attributes = File.GetAttributes(entry);
                }
                catch (FileNotFoundException)
                {
                    continue; // File was deleted after enumeration, skip
                }
                catch (DirectoryNotFoundException)
                {
                    continue; // Directory was deleted after enumeration, skip
                }
                catch (UnauthorizedAccessException)
                {
                    continue; // No permission to access this entry, skip
                }

                var isDir = (attributes & FileAttributes.Directory) != 0;

                long size = 0;
                var lastModified = DateTimeOffset.UtcNow;

                try
                {
                    if (isDir)
                    {
                        var dirInfo = new DirectoryInfo(entry);
                        lastModified = dirInfo.LastWriteTimeUtc;
                    }
                    else
                    {
                        var fileInfo = new FileInfo(entry);
                        size = fileInfo.Length;
                        lastModified = fileInfo.LastWriteTimeUtc;
                    }
                }
                catch (FileNotFoundException)
                {
                    continue; // File/directory deleted, skip
                }
                catch (DirectoryNotFoundException)
                {
                    continue; // Parent directory deleted, skip
                }
                catch (UnauthorizedAccessException)
                {
                    continue; // Cannot read properties, skip
                }

                yield return new StorageItem
                {
                    Uri = itemUri,
                    Size = size,
                    LastModified = lastModified,
                    IsDirectory = isDir,
                };
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
        // Handle UNC paths: file://server/share/path or file://server//share/path are normalized as Host + Path
        if (!string.IsNullOrWhiteSpace(uri.Host))
        {
            // UNC: \\host\path...
            var unc = $"\\\\{uri.Host}{uri.Path.Replace('/', '\\')}";
            return unc;
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
