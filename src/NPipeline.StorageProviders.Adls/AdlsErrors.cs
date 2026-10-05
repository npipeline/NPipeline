using Azure;

namespace NPipeline.StorageProviders.Adls;

/// <summary>Translates <see cref="RequestFailedException" /> into the exception types the storage provider contract names.</summary>
internal static class AdlsErrors
{
    public static Exception Translate(RequestFailedException ex, string filesystem, string path)
    {
        ArgumentNullException.ThrowIfNull(ex);

        var code = ex.ErrorCode ?? string.Empty;
        var status = ex.Status;

        // The HTTP status is authoritative; the error code only decides when the status is missing or generic.
        Exception translated = status switch
        {
            404 => NotFound(ex, filesystem, path, code),
            401 or 403 => AccessDenied(ex, filesystem, path, code),
            400 => Invalid(ex, filesystem, path, code),
            _ => code switch
            {
                "FilesystemNotFound" or "PathNotFound" => NotFound(ex, filesystem, path, code),
                "AuthenticationFailed" or "AuthorizationFailed" or "AuthorizationFailure" or "TokenAuthenticationFailed"
                    => AccessDenied(ex, filesystem, path, code),
                "InvalidQueryParameterValue" or "InvalidResourceName" => Invalid(ex, filesystem, path, code),
                _ => new IOException(
                    $"ADLS operation failed for filesystem '{filesystem}' and path '{path}'. Status={status}, Code={code}. {ex.Message}", ex),
            },
        };

        // Keeps NResilience's record that this failure was already retried.
        foreach (var key in ex.Data.Keys)
        {
            translated.Data[key] = ex.Data[key];
        }

        return translated;
    }

    private static FileNotFoundException NotFound(RequestFailedException ex, string filesystem, string path, string code) =>
        new($"ADLS filesystem '{filesystem}' or path '{path}' not found. Status={ex.Status}, Code={code}.", ex);

    private static UnauthorizedAccessException AccessDenied(RequestFailedException ex, string filesystem, string path, string code) =>
        new($"Access denied to ADLS filesystem '{filesystem}' and path '{path}'. Status={ex.Status}, Code={code}. {ex.Message}", ex);

    private static ArgumentException Invalid(RequestFailedException ex, string filesystem, string path, string code) =>
        new($"Invalid ADLS filesystem '{filesystem}' or path '{path}'. Status={ex.Status}, Code={code}. {ex.Message}", ex);
}
