namespace NPipeline.ErrorHandling;

/// <summary>
///     A factory for creating instances of error handlers and dead-letter sinks.
/// </summary>
public interface IErrorHandlerFactory
{
    /// <summary>
    ///     Creates an instance of the specified dead-letter sink type.
    /// </summary>
    /// <param name="sinkType">The type of the dead-letter sink to create.</param>
    /// <returns>An instance of <see cref="IDeadLetterSink" />, or null if it cannot be created.</returns>
    IDeadLetterSink? CreateDeadLetterSink(Type sinkType);

    /// <summary>
    ///     Whether the caller owns <paramref name="instance" /> and must dispose it. Return false only for instances
    ///     whose lifetime a container manages.
    /// </summary>
    /// <param name="instance">An instance previously returned by <see cref="CreateDeadLetterSink" />.</param>
    /// <returns><see langword="true" /> when the caller owns the instance; otherwise <see langword="false" />.</returns>
    bool CallerOwnsCreatedInstance(object instance) => true;
}
