namespace NPipeline.Graph;

/// <summary>
///     Represents a handle to a join node, which combines two inputs to produce a single output.
/// </summary>
/// <typeparam name="TIn1">The type of the first input.</typeparam>
/// <typeparam name="TIn2">The type of the second input.</typeparam>
/// <typeparam name="TOut">The type of the output.</typeparam>
public sealed record JoinNodeHandle<TIn1, TIn2, TOut>(string Id) : NodeHandle(Id), IOutputNodeHandle<TOut>, INodeHandle;
