using System.Collections;
using System.Net.Sockets;
using Renci.SshNet.Common;

namespace NPipeline.StorageProviders.Sftp;

/// <summary>
///     Translates SSH.NET failures into the exceptions of the storage provider contract. It is the only translator in
///     this package.
/// </summary>
internal static class SftpErrors
{
    /// <summary>Whether <paramref name="exception" /> is a transport or protocol failure that <see cref="Translate" /> handles.</summary>
    internal static bool IsTranslatable(Exception exception) => exception is SshException or SocketException;

    /// <summary>
    ///     Translates <paramref name="exception" />: a missing path to <see cref="FileNotFoundException" />, a permission or
    ///     authentication failure to <see cref="UnauthorizedAccessException" />, and anything else to <see cref="IOException" />,
    ///     each with <paramref name="exception" /> as the inner exception.
    /// </summary>
    /// <param name="exception">The SSH.NET or socket failure.</param>
    /// <param name="host">The server, for the message.</param>
    /// <param name="path">The remote path, for the message.</param>
    internal static Exception Translate(Exception exception, string host, string path)
    {
        ArgumentNullException.ThrowIfNull(exception);

        Exception translated = exception switch
        {
            SftpPathNotFoundException =>
                new FileNotFoundException($"SFTP path '{path}' not found on server '{host}'.", path, exception),

            SftpPermissionDeniedException =>
                new UnauthorizedAccessException($"Access denied to SFTP path '{path}' on server '{host}'. {exception.Message}", exception),

            SshAuthenticationException =>
                new UnauthorizedAccessException($"Authentication failed for SFTP server '{host}'. {exception.Message}", exception),

            SshConnectionException =>
                new IOException($"The connection to SFTP server '{host}' failed. {exception.Message}", exception),

            SshOperationTimeoutException =>
                new IOException($"The operation on SFTP server '{host}' timed out. {exception.Message}", exception),

            _ =>
                new IOException($"SFTP operation failed on server '{host}' for path '{path}'. {exception.Message}", exception),
        };

        foreach (DictionaryEntry entry in exception.Data)
        {
            translated.Data[entry.Key] = entry.Value;
        }

        return translated;
    }
}
