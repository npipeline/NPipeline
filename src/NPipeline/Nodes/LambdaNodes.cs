using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     A transform node that uses a synchronous lambda function for transformation.
/// </summary>
/// <typeparam name="TIn">The input item type.</typeparam>
/// <typeparam name="TOut">The output item type.</typeparam>
/// <remarks>
///     <para>
///         This node is useful for simple, stateless transformations that don't require asynchronous operations.
///         For more complex transformations or those requiring state, consider creating a custom class-based node
///         by inheriting from <see cref="TransformNode{TIn, TOut}" />.
///     </para>
///     <para>
///         The provided delegate is invoked once per input item. If the transformation can fail,
///         consider wrapping it with error handling via the pipeline builder or using
///         <see cref="AsyncLambdaTransformNode{TIn, TOut}" /> for better exception handling.
///     </para>
/// </remarks>
/// <example>
///     <code>
/// // Simple string uppercase transformation
/// var transform = builder.AddTransform(
///     (string s) => s.ToUpperInvariant(),
///     "uppercase");
/// 
/// // Or with a more complex transformation
/// var transform = builder.AddTransform(
///     (Customer c) => new CustomerDto
///     {
///         Id = c.Id,
///         Name = c.Name.Trim()
///     },
///     "customerToDto");
/// </code>
/// </example>
public sealed class LambdaTransformNode<TIn, TOut>(Func<TIn, TOut> transform) : TransformNode<TIn, TOut>
{
    private readonly Func<TIn, TOut> _transform = transform ?? throw new ArgumentNullException(nameof(transform));

    /// <summary>
    ///     Executes the transformation on the input item.
    /// </summary>
    /// <param name="input">The input item to transform.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task containing the transformed item.</returns>
    public override Task<TOut> ExecuteAsync(TIn input, PipelineContext context, CancellationToken cancellationToken)
    {
        return Task.FromResult(_transform(input));
    }

    /// <summary>
    ///     Provides a ValueTask-based execution for optimized performance.
    /// </summary>
    protected internal override ValueTask<TOut> ExecuteValueTaskAsync(TIn input, PipelineContext context, CancellationToken cancellationToken)
    {
        return new ValueTask<TOut>(_transform(input));
    }
}

/// <summary>
///     An asynchronous transform node that uses an async lambda function for transformation.
/// </summary>
/// <typeparam name="TIn">The input item type.</typeparam>
/// <typeparam name="TOut">The output item type.</typeparam>
/// <remarks>
///     <para>
///         This node is useful for transformations that require asynchronous operations such as
///         database queries, HTTP requests, or other I/O operations. The transformation respects
///         the cancellation token and can be cancelled when the pipeline stops.
///     </para>
///     <para>
///         If the transformation is CPU-bound and synchronous, consider using
///         <see cref="LambdaTransformNode{TIn, TOut}" /> instead for better performance.
///     </para>
/// </remarks>
/// <example>
///     <code>
/// // Async HTTP request transformation
/// var transform = builder.AddTransformAsync(
///     async (url, ct) =>
///     {
///         using var client = new HttpClient();
///         var response = await client.GetStringAsync(url, ct);
///         return response;
///     },
///     "fetchUrl");
/// 
/// // Or with database lookup
/// var transform = builder.AddTransformAsync(
///     async (customerId, ct) =>
///     {
///         return await _db.GetCustomerAsync(customerId, ct);
///     },
///     "lookupCustomer");
/// </code>
/// </example>
public sealed class AsyncLambdaTransformNode<TIn, TOut>(
    Func<TIn, CancellationToken, ValueTask<TOut>> transform) : TransformNode<TIn, TOut>
{
    private readonly Func<TIn, CancellationToken, ValueTask<TOut>> _transform =
        transform ?? throw new ArgumentNullException(nameof(transform));

    /// <summary>
    ///     Executes the asynchronous transformation on the input item.
    /// </summary>
    /// <param name="input">The input item to transform.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous transformation.</returns>
    public override async Task<TOut> ExecuteAsync(TIn input, PipelineContext context, CancellationToken cancellationToken)
    {
        return await _transform(input, cancellationToken);
    }

    /// <summary>
    ///     Provides a ValueTask-based execution for optimized performance.
    /// </summary>
    protected internal override async ValueTask<TOut> ExecuteValueTaskAsync(
        TIn input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        return await _transform(input, cancellationToken);
    }
}

/// <summary>
///     A source node that uses a factory function to produce items.
/// </summary>
/// <typeparam name="TOut">The output item type.</typeparam>
/// <remarks>
///     <para>
///         This node is useful for simple data sources such as reading from collections, generating sequences,
///         or reading from files. The factory function should return an async enumerable of items.
///     </para>
///     <para>
///         If the source data comes from a collection, you can use the synchronous overload that automatically
///         converts the enumerable to an async enumerable. For complex source logic or stateful sources,
///         consider creating a custom class-based node by inheriting from <see cref="SourceNode{TOut}" />.
///     </para>
/// </remarks>
/// <example>
///     <code>
/// // Source from a collection
/// var source = builder.AddSource(
///     () => new[] { "apple", "banana", "cherry" },
///     "fruits");
/// 
/// // Source from a file
/// var source = builder.AddSource(
///     async ct =>
///     {
///         var lines = File.ReadAllLinesAsync("/path/to/file.txt", ct);
///         foreach await (var line in lines.WithCancellation(ct))
///         {
///             yield return line;
///         }
///     },
///     "fileLines");
/// </code>
/// </example>
public sealed class LambdaSourceNode<TOut> : SourceNode<TOut>
{
    private readonly Func<CancellationToken, IAsyncEnumerable<TOut>> _factory;

    /// <summary>
    ///     Initializes a new instance of the <see cref="LambdaSourceNode{TOut}" /> class with a synchronous factory.
    /// </summary>
    /// <param name="factory">
    ///     A factory function that returns an enumerable collection of items.
    ///     The enumerable is automatically converted to an async enumerable.
    ///     Must not be null.
    /// </param>
    /// <exception cref="ArgumentNullException">
    ///     Thrown if <paramref name="factory" /> is null.
    /// </exception>
    public LambdaSourceNode(Func<IEnumerable<TOut>> factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        _factory = _ => factory().ToAsyncEnumerable();
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="LambdaSourceNode{TOut}" /> class with an asynchronous factory.
    /// </summary>
    /// <param name="factory">
    ///     A factory function that returns an async enumerable collection of items.
    ///     Must not be null.
    /// </param>
    /// <exception cref="ArgumentNullException">
    ///     Thrown if <paramref name="factory" /> is null.
    /// </exception>
    public LambdaSourceNode(Func<CancellationToken, IAsyncEnumerable<TOut>> factory)
    {
        _factory = factory ?? throw new ArgumentNullException(nameof(factory));
    }

    /// <summary>
    ///     Initializes the source by creating a data pipe from the factory function.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A data pipe containing all items produced by the factory.</returns>
    public override IDataPipe<TOut> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        var items = _factory(cancellationToken);
        return new StreamingDataPipe<TOut>(items);
    }
}

/// <summary>
///     A sink node that uses an action or async function for consuming items.
/// </summary>
/// <typeparam name="TIn">The input item type.</typeparam>
/// <remarks>
///     <para>
///         This node is useful for simple terminal operations such as logging, writing to console,
///         writing to files, or sending to external services. Both synchronous and asynchronous
///         consume operations are supported.
///     </para>
///     <para>
///         If the sink operation is complex or requires state, consider creating a custom class-based node
///         by inheriting from <see cref="SinkNode{TIn}" />.
///     </para>
/// </remarks>
/// <example>
///     <code>
/// // Simple console sink
/// var sink = builder.AddSink(
///     (string line) => Console.WriteLine(line),
///     "console");
/// 
/// // Async file writer sink
/// var sink = builder.AddSinkAsync(
///     async (item, ct) =>
///     {
///         await File.AppendAllTextAsync("/log.txt", item + Environment.NewLine, ct);
///     },
///     "fileWriter");
/// </code>
/// </example>
public sealed class LambdaSinkNode<TIn> : SinkNode<TIn>
{
    private readonly Func<TIn, CancellationToken, ValueTask> _consume;

    /// <summary>
    ///     Initializes a new instance of the <see cref="LambdaSinkNode{TIn}" /> class with a synchronous consumer.
    /// </summary>
    /// <param name="consume">
    ///     The consumer action that processes each input item.
    ///     Must not be null.
    /// </param>
    /// <exception cref="ArgumentNullException">
    ///     Thrown if <paramref name="consume" /> is null.
    /// </exception>
    public LambdaSinkNode(Action<TIn> consume)
    {
        ArgumentNullException.ThrowIfNull(consume);

        _consume = (item, _) =>
        {
            consume(item);
            return ValueTask.CompletedTask;
        };
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="LambdaSinkNode{TIn}" /> class with an asynchronous consumer.
    /// </summary>
    /// <param name="consume">
    ///     The asynchronous consumer function that processes each input item.
    ///     Must not be null.
    /// </param>
    /// <exception cref="ArgumentNullException">
    ///     Thrown if <paramref name="consume" /> is null.
    /// </exception>
    public LambdaSinkNode(Func<TIn, CancellationToken, ValueTask> consume)
    {
        _consume = consume ?? throw new ArgumentNullException(nameof(consume));
    }

    /// <summary>
    ///     Executes the sink by consuming all items from the input pipe.
    /// </summary>
    /// <param name="input">The input data pipe containing items to consume.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous sink operation.</returns>
    public override async Task ExecuteAsync(IDataPipe<TIn> input, PipelineContext context, CancellationToken cancellationToken)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            await _consume(item, cancellationToken);
        }
    }
}
