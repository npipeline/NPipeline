namespace NPipeline.Extensions.Composition
{
    /// <summary>
    ///     Provides naming conventions for composite pipeline node IDs to prevent collisions
    ///     between child sub-pipelines that share the same node names.
    /// </summary>
    public static class CompositeNaming
    {
        /// <summary>
        ///     The separator used between parent and child node IDs.
        /// </summary>
        public const string Separator = "::";

        /// <summary>
        ///     Prefixes a child node ID with the parent composite node's ID
        ///     to produce a globally unique node identifier.
        /// </summary>
        /// <param name="parentNodeId">The composite node's ID in the parent pipeline.</param>
        /// <param name="childNodeId">The node ID within the child sub-pipeline.</param>
        /// <returns>A namespaced node ID in the form <c>parentNodeId::childNodeId</c>.</returns>
        public static string PrefixNodeId(string parentNodeId, string childNodeId)
        {
            return $"{parentNodeId}{Separator}{childNodeId}";
        }

        /// <summary>
        ///     Determines whether a node ID is namespaced (i.e., contains the separator).
        /// </summary>
        /// <param name="nodeId">The node ID to check.</param>
        /// <returns><c>true</c> if the node ID contains the separator; otherwise <c>false</c>.</returns>
        public static bool IsNamespaced(string nodeId)
        {
            return nodeId.Contains(Separator, StringComparison.Ordinal);
        }

        /// <summary>
        ///     Extracts the parent node ID from a namespaced node ID.
        /// </summary>
        /// <param name="namespacedNodeId">A node ID that may contain the separator.</param>
        /// <returns>
        ///     The immediate parent portion (everything before the last separator),
        ///     or <c>null</c> if the ID is not namespaced.
        /// </returns>
        public static string? GetParentNodeId(string namespacedNodeId)
        {
            var separatorIndex = namespacedNodeId.LastIndexOf(Separator, StringComparison.Ordinal);
            return separatorIndex < 0 ? null : namespacedNodeId[..separatorIndex];
        }

        /// <summary>
        ///     Extracts the child node ID from a namespaced node ID.
        /// </summary>
        /// <param name="namespacedNodeId">A node ID that may contain the separator.</param>
        /// <returns>The child portion, or the original ID if not namespaced.</returns>
        public static string GetChildNodeId(string namespacedNodeId)
        {
            var separatorIndex = namespacedNodeId.LastIndexOf(Separator, StringComparison.Ordinal);
            return separatorIndex < 0 ? namespacedNodeId : namespacedNodeId[(separatorIndex + Separator.Length)..];
        }
    }
}
