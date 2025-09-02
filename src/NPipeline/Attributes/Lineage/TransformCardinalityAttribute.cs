namespace NPipeline.Attributes.Lineage;

/// <summary>
///     Describes the logical cardinality of a transformation for lineage purposes.
///     This is advisory; current lineage mapping only enforces OneToOne and can skip
///     mismatch warnings for declared non-OneToOne transforms.
/// </summary>
public enum TransformCardinality
{
    OneToOne = 0,
    OneToZeroOrOne = 1,
    OneToMany = 2,
    ManyToOne = 3,
    ManyToMany = 4,
}

/// <summary>
///     Apply to transform nodes to declare non One-to-One behavior so the lineage
///     mismatch detector can suppress warnings (until advanced mapping is implemented).
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class TransformCardinalityAttribute(TransformCardinality cardinality) : Attribute
{
    public TransformCardinality Cardinality { get; } = cardinality;
}
