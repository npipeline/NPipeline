namespace NPipeline.Extensions.Composition;

/// <summary>
///     Configuration options for composite node sub-pipeline context.
/// </summary>
public sealed class CompositeContextConfiguration
{
    /// <summary>
    ///     Gets the default configuration with observability inheritance enabled.
    ///     Run identity, execution observer, lineage sink, and dead-letter decorator are inherited by default
    ///     so that parent-child telemetry correlates automatically.
    ///     Data container inheritance (parameters, items, properties) remains opt-in.
    /// </summary>
    public static CompositeContextConfiguration Default => new()
    {
        InheritRunIdentity = true,
        InheritExecutionObserver = true,
        InheritLineageSink = true,
        InheritDeadLetterDecorator = true,
    };

    /// <summary>
    ///     Gets or sets whether to inherit parameters from parent context.
    /// </summary>
    public bool InheritParentParameters { get; set; }

    /// <summary>
    ///     Gets or sets whether to inherit items from parent context.
    /// </summary>
    public bool InheritParentItems { get; set; }

    /// <summary>
    ///     Gets or sets whether to inherit properties from parent context.
    /// </summary>
    public bool InheritParentProperties { get; set; }

    /// <summary>
    ///     Gets or sets whether to inherit the run identity (e.g. run ID) from the parent context.
    ///     When true, child pipelines share the same run identity as the parent.
    /// </summary>
    public bool InheritRunIdentity { get; set; }

    /// <summary>
    ///     Gets or sets whether to inherit the lineage sink from the parent context.
    ///     When true, child pipelines report lineage through the same sink as the parent.
    /// </summary>
    public bool InheritLineageSink { get; set; }

    /// <summary>
    ///     Gets or sets whether to inherit the execution observer from the parent context.
    ///     When true, child pipelines emit node lifecycle events through the parent's observer.
    /// </summary>
    public bool InheritExecutionObserver { get; set; }

    /// <summary>
    ///     Gets or sets whether to inherit the dead letter decorator from the parent context.
    ///     When true, child pipelines use the parent's dead letter sink configuration.
    /// </summary>
    public bool InheritDeadLetterDecorator { get; set; }

    /// <summary>
    ///     Creates a new configuration with all inheritance options enabled.
    /// </summary>
    public static CompositeContextConfiguration InheritAll => new()
    {
        InheritParentParameters = true,
        InheritParentItems = true,
        InheritParentProperties = true,
        InheritRunIdentity = true,
        InheritLineageSink = true,
        InheritExecutionObserver = true,
        InheritDeadLetterDecorator = true,
    };
}
