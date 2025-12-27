namespace NPipeline.Pipeline;

/// <summary>
///     Helper extension methods to consolidate disposable registration patterns and satisfy CA2000 by
///     immediately registering newly created async-disposable pipe / node instances with the current pipeline context.
/// </summary>
internal static class PipelineContextDisposalExtensions
{
    /// <summary>
    ///     Registers the object if it implements <see cref="IAsyncDisposable" /> and returns the same instance for fluent usage.
    /// </summary>
    public static T RegisterIfAsyncDisposable<T>(this PipelineContext context, T instance)
    {
        switch (instance)
        {
            case IAsyncDisposable asyncDisposable:
                context.RegisterForDisposal(asyncDisposable);
                break;
            case IDisposable disposable:
#pragma warning disable CA2000 // Wrapper is registered for disposal immediately below
                var wrapper = new AsyncDisposableWrapper(disposable);
#pragma warning restore CA2000
                context.RegisterForDisposal(wrapper);
                break;
        }

        return instance;
    }

    /// <summary>
    ///     Convenience for constructing a disposable/async-disposable instance and immediately registering ownership.
    /// </summary>
    public static T CreateAndRegister<T>(this PipelineContext context, T instance)
    {
        return context.RegisterIfAsyncDisposable(instance);
    }

    private sealed class AsyncDisposableWrapper(IDisposable inner) : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            try
            {
                inner.Dispose();
            }
            catch
            {
                /* swallow */
            }

            return ValueTask.CompletedTask;
        }
    }
}
