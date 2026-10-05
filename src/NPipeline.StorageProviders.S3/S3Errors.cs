using System.Net;
using Amazon.S3;

namespace NPipeline.StorageProviders.S3;

/// <summary>Translates <see cref="AmazonS3Exception" /> into the exception types the storage provider contract names.</summary>
internal static class S3Errors
{
    public static Exception Translate(AmazonS3Exception ex, string bucket, string key)
    {
        // The status code is authoritative; error codes vary by operation (GetObject reports "NoSuchKey", HeadObject "NotFound").
        Exception translated = ex.StatusCode switch
        {
            HttpStatusCode.NotFound => NotFound(ex, bucket, key),
            HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden => AccessDenied(ex, bucket, key),
            HttpStatusCode.BadRequest when ex.ErrorCode is "InvalidBucketName" or "InvalidKey" or "KeyTooLongError" => Invalid(ex, bucket, key),
            _ => ex.ErrorCode switch
            {
                "NoSuchKey" or "NoSuchBucket" or "NotFound" => NotFound(ex, bucket, key),
                "AccessDenied" or "InvalidAccessKeyId" or "SignatureDoesNotMatch" => AccessDenied(ex, bucket, key),
                "InvalidBucketName" or "InvalidKey" => Invalid(ex, bucket, key),
                _ => new IOException($"S3 operation failed for bucket '{bucket}' and key '{key}'. {ex.Message}", ex),
            },
        };

        // Keeps NResilience's record that this failure was already retried.
        foreach (var k in ex.Data.Keys)
        {
            translated.Data[k] = ex.Data[k];
        }

        return translated;
    }

    private static FileNotFoundException NotFound(AmazonS3Exception ex, string bucket, string key) =>
        new($"S3 bucket '{bucket}' or key '{key}' not found.", ex);

    private static UnauthorizedAccessException AccessDenied(AmazonS3Exception ex, string bucket, string key) =>
        new($"Access denied to S3 bucket '{bucket}' and key '{key}'. {ex.Message}", ex);

    private static ArgumentException Invalid(AmazonS3Exception ex, string bucket, string key) =>
        new($"Invalid S3 bucket '{bucket}' or key '{key}'. {ex.Message}", ex);
}
