using NPipeline.StorageProviders.Exceptions;

namespace NPipeline.StorageProviders.Sftp;

/// <summary>
///     Exception thrown when SFTP storage operations fail.
/// </summary>
/// <remarks>
///     Inherits from <see cref="ConnectorException" /> following the established pattern for storage provider exceptions.
/// </remarks>
public sealed class SftpStorageException : ConnectorException
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="SftpStorageException" /> class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="host">The SFTP server host.</param>
    /// <param name="path">The remote path.</param>
    /// <param name="errorCode">The SFTP error code.</param>
    public SftpStorageException(
        string message,
        string? host = null,
        string? path = null,
        SftpErrorCode errorCode = SftpErrorCode.Unknown)
        : base(message)
    {
        Host = host;
        Path = path;
        ErrorCode = errorCode;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="SftpStorageException" /> class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="host">The SFTP server host.</param>
    /// <param name="path">The remote path.</param>
    /// <param name="errorCode">The SFTP error code.</param>
    /// <param name="innerException">The inner exception.</param>
    public SftpStorageException(
        string message,
        string? host,
        string? path,
        SftpErrorCode errorCode,
        Exception innerException)
        : base(message, innerException)
    {
        Host = host;
        Path = path;
        ErrorCode = errorCode;
    }

    /// <summary>
    ///     Gets the SFTP server host.
    /// </summary>
    public string? Host { get; }

    /// <summary>
    ///     Gets the remote path.
    /// </summary>
    public string? Path { get; }

    /// <summary>
    ///     Gets the SFTP error code.
    /// </summary>
    public SftpErrorCode ErrorCode { get; }
}

/// <summary>
///     Error codes for SFTP storage operations.
/// </summary>
public enum SftpErrorCode
{
    /// <summary>
    ///     An unknown error occurred.
    /// </summary>
    Unknown,

    /// <summary>
    ///     Connection to the SFTP server failed.
    /// </summary>
    ConnectionFailed,

    /// <summary>
    ///     Authentication failed.
    /// </summary>
    AuthenticationFailed,

    /// <summary>
    ///     The specified file was not found.
    /// </summary>
    FileNotFound,

    /// <summary>
    ///     Permission denied for the operation.
    /// </summary>
    PermissionDenied,

    /// <summary>
    ///     The specified path was not found.
    /// </summary>
    PathNotFound,

    /// <summary>
    ///     The operation timed out.
    /// </summary>
    OperationTimeout,

    /// <summary>
    ///     The connection was lost during the operation.
    /// </summary>
    ConnectionLost,
}
