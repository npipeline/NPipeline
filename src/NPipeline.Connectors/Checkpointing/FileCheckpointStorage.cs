using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace NPipeline.Connectors.Checkpointing;

/// <summary>
///     File-based checkpoint storage implementation.
///     Stores checkpoints as JSON files in a specified directory.
/// </summary>
public class FileCheckpointStorage : ICheckpointStorage, IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true,
    };

    private readonly string _baseDirectory;
    private readonly bool _createDirectoryIfNotExists;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private bool _disposed;

    /// <summary>
    ///     Initializes a new instance of the <see cref="FileCheckpointStorage" /> class.
    /// </summary>
    /// <param name="baseDirectory">The base directory for storing checkpoint files.</param>
    /// <param name="createDirectoryIfNotExists">Whether to create the directory if it doesn't exist.</param>
    public FileCheckpointStorage(string baseDirectory, bool createDirectoryIfNotExists = true)
    {
        if (string.IsNullOrWhiteSpace(baseDirectory))
            throw new ArgumentException("Base directory cannot be empty.", nameof(baseDirectory));

        _baseDirectory = Path.GetFullPath(baseDirectory);
        _createDirectoryIfNotExists = createDirectoryIfNotExists;

        if (_createDirectoryIfNotExists && !Directory.Exists(_baseDirectory))
            Directory.CreateDirectory(_baseDirectory);
    }

    /// <inheritdoc />
    public async Task<Checkpoint?> LoadAsync(string pipelineId, string nodeId, CancellationToken cancellationToken = default)
    {
        var filePath = GetCheckpointFilePath(pipelineId, nodeId);

        await _lock.WaitAsync(cancellationToken);

        try
        {
            if (!File.Exists(filePath))
                return null;

            var json = await File.ReadAllTextAsync(filePath, cancellationToken);
            var data = JsonSerializer.Deserialize<CheckpointData>(json, JsonOptions);

            if (data == null)
                return null;

            return new Checkpoint(
                data.Value,
                data.Timestamp,
                data.Metadata);
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <inheritdoc />
    public async Task SaveAsync(string pipelineId, string nodeId, Checkpoint checkpoint, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(checkpoint);

        var filePath = GetCheckpointFilePath(pipelineId, nodeId);
        var directoryPath = Path.GetDirectoryName(filePath);

        await _lock.WaitAsync(cancellationToken);

        try
        {
            if (!string.IsNullOrEmpty(directoryPath) && !Directory.Exists(directoryPath))
            {
                if (_createDirectoryIfNotExists)
                    Directory.CreateDirectory(directoryPath);
                else
                    throw new DirectoryNotFoundException($"Checkpoint directory not found: {directoryPath}");
            }

            var data = new CheckpointData
            {
                PipelineId = pipelineId,
                NodeId = nodeId,
                Value = checkpoint.Value,
                Timestamp = checkpoint.Timestamp,
                Metadata = checkpoint.Metadata,
            };

            var json = JsonSerializer.Serialize(data, JsonOptions);
            await File.WriteAllTextAsync(filePath, json, cancellationToken);
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <inheritdoc />
    public async Task DeleteAsync(string pipelineId, string nodeId, CancellationToken cancellationToken = default)
    {
        var filePath = GetCheckpointFilePath(pipelineId, nodeId);

        await _lock.WaitAsync(cancellationToken);

        try
        {
            if (File.Exists(filePath))
                File.Delete(filePath);
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <inheritdoc />
    public async Task<bool> ExistsAsync(string pipelineId, string nodeId, CancellationToken cancellationToken = default)
    {
        var filePath = GetCheckpointFilePath(pipelineId, nodeId);

        await _lock.WaitAsync(cancellationToken);

        try
        {
            return File.Exists(filePath);
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    ///     Disposes resources used by the storage.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _lock.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <summary>
    ///     Gets the file path for a checkpoint.
    /// </summary>
    /// <param name="pipelineId">The pipeline identifier.</param>
    /// <param name="nodeId">The node identifier.</param>
    /// <returns>The full file path.</returns>
    private string GetCheckpointFilePath(string pipelineId, string nodeId)
    {
        // Sanitize identifiers to prevent path traversal
        var safePipelineId = SanitizeIdentifier(pipelineId);
        var safeNodeId = SanitizeIdentifier(nodeId);

        // Create a directory structure: base/pipeline/node.json
        return Path.Combine(_baseDirectory, safePipelineId, $"{safeNodeId}.json");
    }

    /// <summary>
    ///     Sanitizes an identifier for use in file paths.
    /// </summary>
    /// <param name="identifier">The identifier to sanitize.</param>
    /// <returns>A safe identifier string.</returns>
    private static string SanitizeIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            return "unknown";

        // Replace invalid characters with underscores
        var invalidChars = Path.GetInvalidFileNameChars();
        var safe = new StringBuilder(identifier);

        foreach (var c in invalidChars)
        {
            safe.Replace(c, '_');
        }

        // Also replace path separators
        safe.Replace('/', '_').Replace('\\', '_');

        // Hash if too long (using SHA256 instead of MD5 for security)
        if (safe.Length > 100)
        {
            var hash = Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(identifier)));

            return $"checkpoint_{hash}";
        }

        return safe.ToString();
    }

    /// <summary>
    ///     Internal class for JSON serialization.
    /// </summary>
    private sealed class CheckpointData
    {
        public string PipelineId { get; set; } = string.Empty;
        public string NodeId { get; set; } = string.Empty;
        public string Value { get; set; } = string.Empty;
        public DateTimeOffset Timestamp { get; set; }
        public Dictionary<string, string>? Metadata { get; set; }
    }
}
