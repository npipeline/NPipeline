using NPipeline.Graph;

namespace NPipeline.Execution;

public interface ITopologyService
{
    List<string> TopologicalSort(PipelineGraph graph);
    ILookup<string, Edge> BuildInputLookup(PipelineGraph graph);
}
