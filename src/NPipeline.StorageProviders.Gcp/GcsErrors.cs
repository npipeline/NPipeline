using System.Collections;
using System.Net;
using Google;

namespace NPipeline.StorageProviders.Gcp;

/// <summary>
///     Translates Google Cloud Storage failures into the exceptions of the storage provider contract. It is the only
///     translator in this package.
/// </summary>
internal static class GcsErrors
{
    /// <summary>
    ///     Translates <paramref name="exception" />. The HTTP status decides first and the error reason second, because
    ///     the status is always present while the reason is not.
    /// </summary>
    /// <param name="exception">The failure from the Google API client.</param>
    /// <param name="bucket">The bucket involved in the operation.</param>
    /// <param name="objectName">The object name or prefix involved in the operation.</param>
    /// <param name="operation">The operation, for the message.</param>
    /// <returns>
    ///     <see cref="UnauthorizedAccessException" /> for 401 and 403, <see cref="FileNotFoundException" /> for 404,
    ///     <see cref="ArgumentException" /> for 400, and <see cref="IOException" /> for anything else, each with
    ///     <paramref name="exception" /> as the inner exception.
    /// </returns>
    internal static Exception Translate(GoogleApiException exception, string bucket, string objectName, string operation)
    {
        ArgumentNullException.ThrowIfNull(exception);

        var translated = Create(exception, bucket, objectName, operation);

        // Keeps NResilience's record that this failure was already retried, so a pipeline-level retry does not
        // multiply the provider's attempts.
        foreach (DictionaryEntry entry in exception.Data)
        {
            translated.Data[entry.Key] = entry.Value;
        }

        return translated;
    }

    private static Exception Create(GoogleApiException exception, string bucket, string objectName, string operation)
    {
        switch (exception.HttpStatusCode)
        {
            case HttpStatusCode.Unauthorized:
            case HttpStatusCode.Forbidden:
                return new UnauthorizedAccessException(
                    $"Access to GCS bucket '{bucket}' and object '{objectName}' was denied. {exception.Message}", exception);

            case HttpStatusCode.NotFound:
                return new FileNotFoundException($"GCS bucket '{bucket}' or object '{objectName}' not found.", exception);

            case HttpStatusCode.BadRequest:
                return new ArgumentException(
                    $"Invalid request for GCS bucket '{bucket}' and object '{objectName}'. {exception.Message}", exception);
        }

        // No status that settles it: fall back to the error reason.
        var reason = exception.Error?.Errors?.FirstOrDefault()?.Reason;

        return reason switch
        {
            "forbidden" or "authError" or "required" or "accountDisabled"
                => new UnauthorizedAccessException(
                    $"Access to GCS bucket '{bucket}' and object '{objectName}' was denied. {exception.Message}", exception),
            "notFound"
                => new FileNotFoundException($"GCS bucket '{bucket}' or object '{objectName}' not found.", exception),
            "invalid" or "invalidParameter" or "badRequest"
                => new ArgumentException(
                    $"Invalid request for GCS bucket '{bucket}' and object '{objectName}'. {exception.Message}", exception),
            _
                => new IOException(
                    $"Failed to {operation} on GCS bucket '{bucket}' and object '{objectName}'. {exception.Message}", exception),
        };
    }
}
