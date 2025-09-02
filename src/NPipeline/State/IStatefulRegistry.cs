namespace NPipeline.State;

/// <summary>
///     Provides a registry for stateful nodes to enable state persistence and restoration.
/// </summary>
public interface IStatefulRegistry
{
    /// <summary>
    ///     Registers a stateful node with the registry.
    /// </summary>
    /// <param name="nodeId">The unique identifier of the node.</param>
    /// <param name="nodeInstance">The stateful node instance.</param>
    void Register(string nodeId, object nodeInstance);

    /// <summary>
    ///     Unregisters a stateful node from the registry.
    /// </summary>
    /// <param name="nodeId">The unique identifier of the node.</param>
    void Unregister(string nodeId);

    /// <summary>
    ///     Gets all registered stateful nodes.
    /// </summary>
    /// <returns>A dictionary of node IDs to node instances.</returns>
    IReadOnlyDictionary<string, object> GetRegisteredNodes();

    /// <summary>
    ///     Attempts to get a stateful node by its ID.
    /// </summary>
    /// <param name="nodeId">The unique identifier of the node.</param>
    /// <param name="nodeInstance">The stateful node instance if found.</param>
    /// <returns>True if the node was found, false otherwise.</returns>
    bool TryGetNode(string nodeId, out object? nodeInstance);
}
