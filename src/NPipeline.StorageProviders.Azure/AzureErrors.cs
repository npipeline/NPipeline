using Azure;
using NPipeline.StorageProviders.Exceptions;

namespace NPipeline.StorageProviders.Azure;

/// <summary>Translates <see cref="RequestFailedException" /> into the exception types the storage provider contract names.</summary>
internal static class AzureErrors
{
    public static Exception Translate(RequestFailedException ex, string container, string blob)
    {
        ArgumentNullException.ThrowIfNull(ex);

        var code = ex.ErrorCode ?? string.Empty;
        var status = ex.Status;

        // The HTTP status is authoritative; the error code only decides when the status is missing or generic.
        Exception translated = status switch
        {
            404 => NotFound(ex, container, blob, code),
            401 or 403 => AccessDenied(ex, container, blob, code),
            412 => Precondition(ex),
            409 when code is "BlobAlreadyExists" or "PathAlreadyExists" => Precondition(ex),
            400 => Invalid(ex, container, blob, code),
            _ => code switch
            {
                "ContainerNotFound" or "BlobNotFound" => NotFound(ex, container, blob, code),
                "AuthenticationFailed" or "AuthorizationFailed" or "AuthorizationFailure" or "TokenAuthenticationFailed"
                    => AccessDenied(ex, container, blob, code),
                "InvalidQueryParameterValue" or "InvalidResourceName" => Invalid(ex, container, blob, code),
                _ => new IOException(
                    $"Azure operation failed for container '{container}' and blob '{blob}'. Status={status}, Code={code}. {ex.Message}", ex),
            },
        };

        // Keeps NResilience's record that this failure was already retried.
        foreach (var key in ex.Data.Keys)
        {
            translated.Data[key] = ex.Data[key];
        }

        return translated;
    }

    private static FileNotFoundException NotFound(RequestFailedException ex, string container, string blob, string code) =>
        new($"Azure container '{container}' or blob '{blob}' not found. Status={ex.Status}, Code={code}.", ex);

    private static UnauthorizedAccessException AccessDenied(RequestFailedException ex, string container, string blob, string code) =>
        new($"Access denied to Azure container '{container}' and blob '{blob}'. Status={ex.Status}, Code={code}. {ex.Message}", ex);

    private static ArgumentException Invalid(RequestFailedException ex, string container, string blob, string code) =>
        new($"Invalid Azure container '{container}' or blob '{blob}'. Status={ex.Status}, Code={code}. {ex.Message}", ex);

    private static StoragePreconditionFailedException Precondition(RequestFailedException ex) =>
        new($"Azure refused the conditional write: the object changed or already exists. {ex.Message}", ex);
}
