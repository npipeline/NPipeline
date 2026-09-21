namespace NPipeline.Pipeline;

/// <summary>
///     Creates pipelines from definitions.
/// </summary>
public sealed class PipelineFactory : IPipelineFactory
{
    /// <inheritdoc />
    public Pipeline Create<TDefinition>(PipelineContext context) where TDefinition : IPipelineDefinition, new()
    {
        ArgumentNullException.ThrowIfNull(context);
        return BuildPipeline(new TDefinition(), context);
    }

    /// <inheritdoc />
    public Pipeline Create(IPipelineDefinition definition, PipelineContext context)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(context);

        return BuildPipeline(definition, context);
    }

    private static Pipeline BuildPipeline(IPipelineDefinition definition, PipelineContext context)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(context);

        // Build with the lineage module the run will actually use, so build-time adapters and runtime lineage
        // handling cannot come from different instances.
        var builder = new PipelineBuilder(context.Lineage.Module);

        definition.Define(builder, context);

        // Allow tests / advanced users to supply preconfigured node instances via context.
        if (context.PreconfiguredNodeInstances.Count > 0)
        {
            foreach (var kvp in context.PreconfiguredNodeInstances)
            {
                // Best-effort: ignore duplicates (will throw) so wrap in try/catch.
                try
                {
                    builder.AddPreconfiguredNodeInstance(kvp.Key, kvp.Value);
                }
                catch
                {
                    /* ignore */
                }
            }
        }

        return builder.Build();
    }
}
