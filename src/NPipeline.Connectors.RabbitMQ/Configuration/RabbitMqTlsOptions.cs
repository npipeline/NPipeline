using System.Security.Authentication;

namespace NPipeline.Connectors.RabbitMQ.Configuration;

/// <summary>
///     TLS configuration for RabbitMQ connections.
/// </summary>
public sealed record RabbitMqTlsOptions
{
    /// <summary>
    ///     Gets or sets whether TLS is enabled. Default is false.
    /// </summary>
    public bool Enabled { get; init; }

    /// <summary>
    ///     Gets or sets the server name for TLS certificate validation.
    /// </summary>
    public string? ServerName { get; init; }

    /// <summary>
    ///     Gets or sets the path to the client certificate file.
    /// </summary>
    public string? CertificatePath { get; init; }

    /// <summary>
    ///     Gets or sets the passphrase for the client certificate.
    /// </summary>
    public string? CertificatePassphrase { get; init; }

    /// <summary>
    ///     Gets or sets the TLS protocol version. Default is TLS 1.2.
    /// </summary>
    public SslProtocols SslProtocols { get; init; } = SslProtocols.Tls12;
}
