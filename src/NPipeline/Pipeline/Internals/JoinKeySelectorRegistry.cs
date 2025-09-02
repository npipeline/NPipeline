using System.Collections.Concurrent;
using NPipeline.Graph;

namespace NPipeline.Pipeline.Internals;

/// <summary>
///     Registry for pre-compiled join key selectors to enable zero-overhead access at runtime.
/// </summary>
/// <remarks>
///     This registry is populated during the builder phase when join nodes are added,
///     and consulted at runtime by BaseJoinNode to avoid reflection and expression tree compilation.
///     Uses Type object as key to avoid string allocation and handle edge cases with generic nested types.
/// </remarks>
internal static class JoinKeySelectorRegistry
{
    private static readonly ConcurrentDictionary<Type, (JoinKeySelectorDelegate Selector1, JoinKeySelectorDelegate Selector2)>
        _cache = new();

    /// <summary>
    ///     Registers pre-compiled key selectors for a specific join node type.
    /// </summary>
    /// <remarks>
    ///     Uses Type object as key for O(1) lookup and to avoid string allocation or null-for-generic-nested edge cases.
    /// </remarks>
    public static void Register(
        Type joinNodeType,
        JoinKeySelectorDelegate selector1,
        JoinKeySelectorDelegate selector2)
    {
        _cache.TryAdd(joinNodeType, (selector1, selector2));
    }

    /// <summary>
    ///     Tries to retrieve pre-compiled key selectors for a join node type.
    /// </summary>
    /// <remarks>
    ///     Returns false if selectors have not been pre-compiled for this node type,
    ///     allowing fallback to runtime compilation.
    /// </remarks>
    public static bool TryGetSelectors(
        Type joinNodeType,
        out JoinKeySelectorDelegate? selector1,
        out JoinKeySelectorDelegate? selector2)
    {
        if (_cache.TryGetValue(joinNodeType, out var selectors))
        {
            selector1 = selectors.Selector1;
            selector2 = selectors.Selector2;
            return true;
        }

        selector1 = null;
        selector2 = null;
        return false;
    }
}
