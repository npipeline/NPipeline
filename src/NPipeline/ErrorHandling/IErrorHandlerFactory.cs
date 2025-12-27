namespace NPipeline.ErrorHandling;

/// <summary>
///     A factory for creating instances of error handlers and dead-letter sinks.
/// </summary>
public interface IErrorHandlerFactory
{
    /// <summary>
    ///     Creates an instance of the specified error handler type.
    /// </summary>
    /// <param name="handlerType">The type of the error handler to create.</param>
    /// <returns>An instance of <see cref="IPipelineErrorHandler" />, or null if it cannot be created.</returns>
    IPipelineErrorHandler? CreateErrorHandler(Type handlerType);

    /// <summary>
    ///     Creates an instance of the specified node error handler type.
    /// </summary>
    /// <param name="handlerType">The type of the error handler to create.</param>
    /// <returns>An instance of <see cref="INodeErrorHandler" />, or null if it cannot be created.</returns>
    INodeErrorHandler? CreateNodeErrorHandler(Type handlerType);

    /// <summary>
    ///     Creates an instance of the specified dead-letter sink type.
    /// </summary>
    /// <param name="sinkType">The type of the dead-letter sink to create.</param>
    /// <returns>An instance of <see cref="IDeadLetterSink" />, or null if it cannot be created.</returns>
    IDeadLetterSink? CreateDeadLetterSink(Type sinkType);
}
