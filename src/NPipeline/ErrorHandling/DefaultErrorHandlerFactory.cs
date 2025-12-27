using NPipeline.Observability.Logging;

namespace NPipeline.ErrorHandling;

/// <summary>
///     Default implementation of <see cref="IErrorHandlerFactory" /> that creates error handlers and dead-letter sinks
///     using reflection with proper error handling and logging.
/// </summary>
public sealed class DefaultErrorHandlerFactory : IErrorHandlerFactory
{
    private readonly IPipelineLogger _logger;

    /// <summary>
    ///     Initializes a new instance of the <see cref="DefaultErrorHandlerFactory" /> class.
    /// </summary>
    /// <param name="loggerFactory">Optional logger factory for diagnostic logging. Defaults to NullPipelineLoggerFactory if not provided.</param>
    public DefaultErrorHandlerFactory(IPipelineLoggerFactory? loggerFactory = null)
    {
        var factory = loggerFactory ?? NullPipelineLoggerFactory.Instance;
        _logger = factory.CreateLogger(nameof(DefaultErrorHandlerFactory));
    }

    /// <summary>
    ///     Creates an instance of the specified error handler type.
    /// </summary>
    /// <param name="handlerType">The type of the error handler to create.</param>
    /// <returns>An instance of <see cref="IPipelineErrorHandler" />, or null if it cannot be created.</returns>
    public IPipelineErrorHandler? CreateErrorHandler(Type handlerType)
    {
        return TryCreateInstance<IPipelineErrorHandler>(handlerType, nameof(CreateErrorHandler));
    }

    /// <summary>
    ///     Creates an instance of the specified node error handler type.
    /// </summary>
    /// <param name="handlerType">The type of the error handler to create.</param>
    /// <returns>An instance of <see cref="INodeErrorHandler" />, or null if it cannot be created.</returns>
    public INodeErrorHandler? CreateNodeErrorHandler(Type handlerType)
    {
        return TryCreateInstance<INodeErrorHandler>(handlerType, nameof(CreateNodeErrorHandler));
    }

    /// <summary>
    ///     Creates an instance of the specified dead-letter sink type.
    /// </summary>
    /// <param name="sinkType">The type of the dead-letter sink to create.</param>
    /// <returns>An instance of <see cref="IDeadLetterSink" />, or null if it cannot be created.</returns>
    public IDeadLetterSink? CreateDeadLetterSink(Type sinkType)
    {
        return TryCreateInstance<IDeadLetterSink>(sinkType, nameof(CreateDeadLetterSink));
    }

    /// <summary>
    ///     Attempts to create an instance of the specified type using reflection.
    /// </summary>
    /// <typeparam name="T">The interface type expected.</typeparam>
    /// <param name="type">The concrete type to instantiate.</param>
    /// <param name="methodName">The name of the calling method for diagnostics.</param>
    /// <returns>The created instance or null if creation fails.</returns>
    private T? TryCreateInstance<T>(Type type, string methodName) where T : class
    {
        string? error = null;
        T? instance = null;

        if (!typeof(T).IsAssignableFrom(type))
            error = $"Type {type.FullName} does not implement {typeof(T).FullName}";
        else
        {
            try
            {
                instance = Activator.CreateInstance(type) as T;

                if (instance is null)
                    error = $"Activator returned null for {type.FullName}";
            }
            catch (Exception ex)
            {
                error = ex.Message;
            }
        }

        if (instance is not null)
            return instance;

        _logger.Log(
            LogLevel.Warning,
            "{Factory}.{Method}: Failed to create instance of {Type}: {Message}",
            nameof(DefaultErrorHandlerFactory),
            methodName,
            type?.FullName ?? "null",
            error ?? "unknown error");

        return null;
    }
}
