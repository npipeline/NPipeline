using NPipeline.Execution.Plans;
using NPipeline.Graph;
using NPipeline.Lineage;
using NPipeline.Nodes;

namespace NPipeline.Execution.Orchestration;

internal readonly record struct PipelineExecutionSetupResult(
    PipelineGraph Graph,
    Dictionary<string, INode> NodeInstances,
    IReadOnlyDictionary<string, NodeDefinition> NodeDefinitionMap,
    IReadOnlyDictionary<string, NodeExecutionPlan> ExecutionPlans,
    IPipelineLineageSink? PipelineLineageSink);
