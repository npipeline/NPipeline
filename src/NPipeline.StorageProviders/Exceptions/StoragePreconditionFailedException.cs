namespace NPipeline.StorageProviders.Exceptions;

/// <summary>
///     A conditional write was refused: the target exists although <see cref="Models.StorageWriteOptions.Overwrite" /> is
///     <see langword="false" />, or its ETag no longer matches <see cref="Models.StorageWriteOptions.IfMatch" />. Another writer
///     committed first; re-read the object and try again.
/// </summary>
public sealed class StoragePreconditionFailedException : IOException
{
    /// <summary>Creates the exception.</summary>
    /// <param name="message">What was refused.</param>
    /// <param name="innerException">The store's exception, when there is one.</param>
    public StoragePreconditionFailedException(string message, Exception? innerException = null)
        : base(message, innerException)
    {
    }
}
