namespace NPipeline.Attributes.Lineage;

/// <summary>
///     Describes the logical cardinality of a transformation for lineage purposes.
///     This is advisory; current lineage mapping only enforces OneToOne and can skip
///     mismatch warnings for declared non-OneToOne transforms.
/// </summary>
public enum TransformCardinality
{
    /// <summary>
    ///     One input item produces exactly one output item.
    /// </summary>
    OneToOne = 0,


    /// <summary>
    ///     One input item may produce zero or one output item.
    /// </summary>
    OneToZeroOrOne = 1,


    /// <summary>
    ///     One input item produces multiple output items.
    /// </summary>
    OneToMany = 2,


    /// <summary>
    ///     Multiple input items produce one output item.
    /// </summary>
    ManyToOne = 3,


    /// <summary>
    ///     Multiple input items produce multiple output items.
    /// </summary>
    ManyToMany = 4,
}

/// <summary>
///     Apply to transform nodes to declare non One-to-One behavior so the lineage
///     mismatch detector can suppress warnings (until advanced mapping is implemented).
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class TransformCardinalityAttribute(TransformCardinality cardinality) : Attribute
{
    /// <summary>
    ///     Gets the declared cardinality of the transformation.
    /// </summary>
    public TransformCardinality Cardinality { get; } = cardinality;
}
