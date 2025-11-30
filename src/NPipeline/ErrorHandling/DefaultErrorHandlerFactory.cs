using System.Diagnostics;
using NPipeline.Utils;

namespace NPipeline.ErrorHandling;

/// <summary>
///     Default implementation of <see cref="IErrorHandlerFactory" /> that creates error handlers and dead-letter sinks
///     using reflection with proper error handling and logging.
/// </summary>
internal sealed class DefaultErrorHandlerFactory : IErrorHandlerFactory
{
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
    ///     Attempts to create an instance of the specified type using the centralized InstanceFactory.
    /// </summary>
    /// <typeparam name="T">The interface type expected.</typeparam>
    /// <param name="type">The concrete type to instantiate.</param>
    /// <param name="methodName">The name of the calling method for diagnostics.</param>
    /// <returns>The created instance or null if creation fails.</returns>
    private static T? TryCreateInstance<T>(Type type, string methodName) where T : class
    {
        if (InstanceFactory.TryCreate<T>(type, out var instance, out var error))
            return instance;

        Debug.WriteLine($"DefaultErrorHandlerFactory.{methodName}: Failed to create instance of {type?.FullName ?? "null"}: {error}");
        return null;
    }
}
