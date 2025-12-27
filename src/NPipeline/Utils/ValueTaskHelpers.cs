namespace NPipeline.Utils;

/// <summary>
///     Helper methods for bridging between <see cref="ValueTask" /> and <see cref="Task" /> while keeping hot paths allocation friendly.
/// </summary>
internal static class ValueTaskHelpers
{
    /// <summary>
    ///     Converts a <see cref="ValueTask{TResult}" /> to a <see cref="Task{TResult}" />, creating a new task only when necessary.
    /// </summary>
    /// <typeparam name="T">The result type.</typeparam>
    /// <param name="valueTask">The source value task.</param>
    /// <returns>A task representing the same asynchronous operation.</returns>
    public static Task<T> ToTask<T>(ValueTask<T> valueTask)
    {
        if (valueTask.IsCompletedSuccessfully)
            return Task.FromResult(valueTask.Result);

        return valueTask.AsTask();
    }
}
