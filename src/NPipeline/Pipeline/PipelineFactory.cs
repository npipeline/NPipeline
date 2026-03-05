using NPipeline.Nodes;

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

        var definition = new TDefinition();

        var builder = new PipelineBuilder();

        definition.Define(builder, context);

        // Allow tests / advanced users to supply preconfigured node instances via context.
        if (context.PreconfiguredNodeInstances.Count > 0)
        {
            foreach (var kvp in context.PreconfiguredNodeInstances)

            // Best-effort: ignore duplicates (will throw) so wrap in try/catch.
            {
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
