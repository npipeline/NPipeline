// ReSharper disable once CheckNamespace

namespace NPipeline;

/// <summary>
///     Provides extension methods for async enumeration and stream processing.
/// </summary>
public static class AsyncExtensions
{
#if !NET10_0
#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously

    /// <summary>
    ///     Converts a synchronous enumerable to an asynchronous enumerable.
    /// </summary>
    /// <typeparam name="T">The type of items in the enumerable.</typeparam>
    /// <param name="source">The synchronous enumerable to convert.</param>
    /// <returns>An async enumerable that yields the same items as the source.</returns>
    /// <remarks>
    ///     <para>
    ///         This method allows synchronous collections (lists, arrays, etc.) to be used
    ///         in pipeline contexts that require <see cref="IAsyncEnumerable{T}" />.
    ///     </para>
    ///     <para>
    ///         The conversion is non-blocking—items are yielded synchronously, but the
    ///         method itself is declared as async to satisfy API contracts.
    ///     </para>
    /// </remarks>
    /// <example>
    ///     <code>
    /// // Convert a list to async enumerable
    /// var items = new List&lt;int&gt; { 1, 2, 3, 4, 5 };
    /// var asyncItems = items.ToAsyncEnumerable();
    /// 
    /// // Can now be used in foreach loops with await
    /// await foreach (var item in asyncItems)
    /// {
    ///     Console.WriteLine(item);
    /// }
    /// </code>
    /// </example>
    public static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(this IEnumerable<T> source)
    {
        foreach (var item in source)
        {
            yield return item;
        }
    }
#pragma warning restore CS1998
#endif
}
