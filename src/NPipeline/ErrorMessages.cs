#pragma warning disable IDE0022 // Use expression body for methods

namespace NPipeline;

/// <summary>
///     Centralized error messages for NPipeline exceptions.
///     Each message includes an error code for documentation lookup and actionable guidance.
///     Error codes are organized by category:
///     - NP01xx: Graph Validation Errors
///     - NP02xx: Type Mismatch and Conversion Errors
///     - NP03xx: Execution Errors
///     - NP04xx: Configuration Errors
///     - NP05xx: Resource and Capacity Errors
/// </summary>
internal static class ErrorMessages
{
    private const string DocsBaseUrl = "https://github.com/npipeline/NPipeline/docs/reference/error-codes";

    #region Analyzer Diagnostics (NP90xx-NP94xx)

    public static string UnsafePipelineContextAccess(string accessPattern, string recommendedPattern)
    {
        return $"[{ErrorCodes.UnsafePipelineContextAccess}] Unsafe access pattern detected on PipelineContext. " +
               $"Access pattern: {accessPattern}. " +
               $"This can lead to NullReferenceException at runtime. " +
               $"Recommended pattern: {recommendedPattern}. " +
               $"Use null-conditional operators (?.) or check for null before accessing these properties. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.UnsafePipelineContextAccess}";
    }

    #endregion

    #region Graph Validation Errors (NP01xx)

    public static string PipelineRequiresAtLeastOneNode()
    {
        return $"[{ErrorCodes.PipelineRequiresAtLeastOneNode}] A pipeline must have at least one node. " +
               $"Add at least one node (source, transform, or sink) before building. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.PipelineRequiresAtLeastOneNode}";
    }

    public static string NodeMissingInputConnection(string nodeId, string nodeName, string nodeKind)
    {
        return $"[{ErrorCodes.NodeMissingInputConnection}] Node '{nodeId}' ({nodeName}, {nodeKind}) is missing a required input connection. " +
               $"Transform and sink nodes must have at least one incoming connection from another node. " +
               $"Ensure all nodes except sources have inputs connected. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.NodeMissingInputConnection}";
    }

    public static string CyclicDependencyDetected()
    {
        return $"[{ErrorCodes.CyclicDependencyDetected}] Cyclic dependency detected in pipeline graph. " +
               $"Pipelines must be directed acyclic graphs (DAGs) without cycles. " +
               $"Check your node connections and remove any circular references. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.CyclicDependencyDetected}";
    }

    public static string NodeAlreadyAdded(string nodeId)
    {
        return $"[{ErrorCodes.NodeAlreadyAdded}] A node with ID '{nodeId}' has already been added to the pipeline. " +
               $"Each node ID must be unique within a pipeline. " +
               $"Use a different ID or check your builder configuration. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.NodeAlreadyAdded}";
    }

    public static string NodeNameNotUnique(string name)
    {
        return $"[{ErrorCodes.NodeNameNotUnique}] A node with name '{name}' has already been added to the pipeline. " +
               $"Node names must be unique within a pipeline. " +
               $"Either provide a different name or let the framework auto-generate one. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.NodeNameNotUnique}";
    }

    #endregion

    #region Type Mismatch and Conversion Errors (NP02xx)

    public static string TypeMismatchInConnection(string sourceNodeId, Type sourceType, string targetNodeId, Type targetType)
    {
        return $"[{ErrorCodes.TypeMismatchInConnection}] Type mismatch in connection between nodes '{sourceNodeId}' and '{targetNodeId}'. " +
               $"Output type '{sourceType.Name}' is not compatible with input type '{targetType.Name}'. " +
               $"Consider adding a transformation node between them. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.TypeMismatchInConnection}";
    }

    public static string InputDataPipeWrongType(string expectedType, string actualType, string nodeName)
    {
        return $"[{ErrorCodes.InputDataPipeWrongType}] Input data pipe is not of the expected type for node '{nodeName}'. " +
               $"Expected '{expectedType}' but found '{actualType}'. " +
               $"This usually indicates a graph construction error. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.InputDataPipeWrongType}";
    }

    public static string CannotRegisterMappingsAfterExecution(string nodeName)
    {
        return $"[{ErrorCodes.CannotRegisterMappingsAfterExecution}] Cannot register type mappings after execution has begun for node '{nodeName}'. " +
               $"All type mappings and conversions must be configured before the node starts executing. " +
               $"Configure mappings in the builder or before the first item arrives. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.CannotRegisterMappingsAfterExecution}";
    }

    public static string RecordTypeHasNoPublicConstructor(string recordTypeName)
    {
        return $"[{ErrorCodes.RecordTypeHasNoPublicConstructor}] Record type '{recordTypeName}' has no public constructors. " +
               $"Cannot construct records without public constructors. " +
               $"Either add a public constructor or use a different conversion method. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.RecordTypeHasNoPublicConstructor}";
    }

    public static string InvalidMemberAccessExpression(string selector)
    {
        return $"[{ErrorCodes.InvalidMemberAccessExpression}] Member selector must be a member access expression. " +
               $"Received: {selector}. " +
               $"Use expressions like 'x => x.Property' or 'x => x.Field'. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.InvalidMemberAccessExpression}";
    }

    public static string MemberNotWritable(string memberPath)
    {
        return $"[{ErrorCodes.MemberNotWritable}] Member '{memberPath}' cannot be written to (no public setter or readonly). " +
               $"Ensure the target member is a writable property or field. " +
               $"Check field initializers and property definitions. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.MemberNotWritable}";
    }

    public static string SetterCreationFailed(string memberPath, string details)
    {
        return $"[{ErrorCodes.SetterCreationFailed}] Failed to create setter for '{memberPath}'. " +
               $"Details: {details}. " +
               $"This may indicate a reflection limitation with the target member type. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.SetterCreationFailed}";
    }

    public static string ValueTupleConstructorNotFound(string keyTypeName)
    {
        return $"[{ErrorCodes.ValueTupleConstructorNotFound}] Could not find a public constructor for ValueTuple key type '{keyTypeName}'. " +
               $"ValueTuple types require accessible constructors. " +
               $"Ensure all component types in the tuple are properly supported. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.ValueTupleConstructorNotFound}";
    }

    public static string CannotConcatenateStreamsTypeMismatch(string expectedType, string foundType)
    {
        return $"[{ErrorCodes.CannotConcatenateStreamsTypeMismatch}] Cannot concatenate streams due to type mismatch. " +
               $"Expected pipe of '{expectedType}', but found '{foundType}'. " +
               $"All inputs to a merge point must have compatible types. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.CannotConcatenateStreamsTypeMismatch}";
    }

    #endregion

    #region Execution Errors (NP03xx)

    public static string NodeKindNotSupported(string nodeKind)
    {
        return $"[{ErrorCodes.NodeKindNotSupported}] Node kind '{nodeKind}' is not supported or its execution delegate is missing. " +
               $"This typically indicates an incomplete node registration in the framework. " +
               $"Ensure all node types are properly registered. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.NodeKindNotSupported}";
    }

    public static string OutputNotFoundForSourceNode(string sourceNodeId)
    {
        return $"[{ErrorCodes.OutputNotFoundForSourceNode}] Could not find output for source node '{sourceNodeId}'. " +
               $"The source node may not have executed or data may not be available. " +
               $"Check that the source node executed successfully before consuming its output. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.OutputNotFoundForSourceNode}";
    }

    public static string PipelineExecutionFailedAtNode(string nodeId, Exception innerException)
    {
        return $"[{ErrorCodes.PipelineExecutionFailedAtNode}] Pipeline execution failed at node '{nodeId}'. " +
               $"Inner error: {innerException.Message}. " +
               $"Check the node's logic and the error context for details. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.PipelineExecutionFailedAtNode}";
    }

    public static string PipelineExecutionFailed(string pipelineTypeName, Exception innerException)
    {
        return $"[{ErrorCodes.PipelineExecutionFailed}] Pipeline execution failed for '{pipelineTypeName}'. " +
               $"Inner error: {innerException.Message}. " +
               $"Review the error trace and check your pipeline definition. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.PipelineExecutionFailed}";
    }

    public static string ItemFailedAfterMaxRetries(int attempts, Exception originalException)
    {
        return $"[{ErrorCodes.ItemFailedAfterMaxRetries}] An item failed to process after {attempts} attempts. " +
               $"Original error: {originalException.Message}. " +
               $"Item retry limit exhausted. Check error handling or increase retry limits. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.ItemFailedAfterMaxRetries}";
    }

    public static string ErrorHandlingFailed(string nodeId, Exception handlerException)
    {
        return $"[{ErrorCodes.ErrorHandlingFailed}] Error handling failed for node '{nodeId}'. " +
               $"Handler error: {handlerException.Message}. " +
               $"The error handler itself threw an exception. Review your error handler implementation. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.ErrorHandlingFailed}";
    }

    public static string LineageCardinalityMismatch(string nodeId, int inputCount, int outputCount)
    {
        return $"[{ErrorCodes.LineageCardinalityMismatch}] Lineage cardinality mismatch in node '{nodeId}'. " +
               $"Inputs: {inputCount}, Outputs: {outputCount}. " +
               $"The number of lineage mappings doesn't match inputs/outputs. " +
               $"Check the node's lineage configuration. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.LineageCardinalityMismatch}";
    }

    public static string FailedToExtractItemsFromInMemoryDataPipe(Exception innerException)
    {
        return $"[{ErrorCodes.FailedToExtractItemsFromInMemoryDataPipe}] Failed to extract items from InMemoryDataPipe. " +
               $"Inner error: {innerException.Message}. " +
               $"The pipe may be corrupted or disposed. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.FailedToExtractItemsFromInMemoryDataPipe}";
    }

    public static string CircuitBreakerTripped(int failureThreshold, string nodeId)
    {
        return $"[{ErrorCodes.CircuitBreakerTripped}] Circuit breaker tripped for node '{nodeId}' after {failureThreshold} consecutive failures. " +
               $"The node has been temporarily disabled to prevent cascading failures. " +
               $"Either fix the underlying issue or increase the failure threshold. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.CircuitBreakerTripped}";
    }

    public static string RetryLimitExhausted(string nodeId, int maxAttempts, int consecutiveFailures)
    {
        return $"[{ErrorCodes.RetryLimitExhausted}] Retry limit exhausted for node '{nodeId}'. " +
               $"Attempted {maxAttempts} times with {consecutiveFailures} consecutive failures. " +
               $"The node cannot recover. Review error logs and the node implementation. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.RetryLimitExhausted}";
    }

    #endregion

    #region Configuration Errors (NP04xx)

    public static string ExecutionStrategyCannotBeSetForNonTransformNode(string nodeName, string nodeKind)
    {
        return $"[{ErrorCodes.ExecutionStrategyCannotBeSetForNonTransformNode}] Execution strategy can only be set for transform nodes. " +
               $"Node '{nodeName}' is a {nodeKind} node. " +
               $"Remove the execution strategy configuration or change the node type. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.ExecutionStrategyCannotBeSetForNonTransformNode}";
    }

    public static string NodeNotFoundInBuilder(string nodeId, string operation)
    {
        return
            $"[{ErrorCodes.NodeNotFoundInBuilder}] Cannot perform operation '{operation}' on node '{nodeId}' because it has not been added to the builder. " +
            $"Ensure the node is registered before configuring it. " +
            $"See: {DocsBaseUrl}#{ErrorCodes.NodeNotFoundInBuilder}";
    }

    public static string ResilienceCannotBeAppliedToNonTransformNode(string nodeName, string nodeKind)
    {
        return $"[{ErrorCodes.ResilienceCannotBeAppliedToNonTransformNode}] Resilience can only be applied to transform nodes. " +
               $"Node '{nodeName}' is a {nodeKind} node. " +
               $"Remove resilience configuration or change the node type. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.ResilienceCannotBeAppliedToNonTransformNode}";
    }

    public static string InvalidErrorHandlerType(string typeName)
    {
        return $"[{ErrorCodes.InvalidErrorHandlerType}] The provided type '{typeName}' does not implement INodeErrorHandler<,>. " +
               $"Custom error handlers must implement the INodeErrorHandler interface with correct type parameters. " +
               $"Review your error handler class definition. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.InvalidErrorHandlerType}";
    }

    public static string PreConfiguredInstanceAlreadyAdded(string nodeId)
    {
        return $"[{ErrorCodes.PreConfiguredInstanceAlreadyAdded}] A pre-configured instance for node '{nodeId}' has already been added. " +
               $"Each node can have only one pre-configured instance. " +
               $"Either reuse the existing instance or register a new node. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.PreConfiguredInstanceAlreadyAdded}";
    }

    public static string PreConfiguredInstanceNodeNotFound(string nodeId)
    {
        return
            $"[{ErrorCodes.PreConfiguredInstanceNodeNotFound}] Cannot add pre-configured instance for node '{nodeId}' because it has not been added to the builder. " +
            $"Register the node first, then add its pre-configured instance. " +
            $"See: {DocsBaseUrl}#{ErrorCodes.PreConfiguredInstanceNodeNotFound}";
    }

    public static string MergeStrategyNotSupported(string mergeStrategyName)
    {
        return $"[{ErrorCodes.MergeStrategyNotSupported}] Merge strategy '{mergeStrategyName}' is not supported. " +
               $"Use a supported merge strategy or implement a custom one. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.MergeStrategyNotSupported}";
    }

    public static string NodeActivationFailed(string nodeTypeName, string reason)
    {
        return $"[{ErrorCodes.NodeActivationFailed}] Cannot instantiate node of type '{nodeTypeName}'. " +
               $"Reason: {reason}. " +
               $"Ensure the node type has a public parameterless constructor or is registered with DI. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.NodeActivationFailed}";
    }

    public static string JoinNodeRequiresTwoKeySelectorAttributes()
    {
        return
            $"[{ErrorCodes.JoinNodeRequiresTwoKeySelectorAttributes}] Join node requires exactly two KeySelectorAttribute declarations, one for each input type. " +
            $"Ensure your join node class has decorators for both input types. " +
            $"See: {DocsBaseUrl}#{ErrorCodes.JoinNodeRequiresTwoKeySelectorAttributes}";
    }

    public static string UnbatchingNodeNotSupported()
    {
        return $"[{ErrorCodes.UnbatchingNodeNotSupported}] UnbatchingNode should not be executed directly via ExecuteAsync. " +
               $"UnbatchingNode is designed to be used internally through the pipeline framework. " +
               $"Use PipelineBuilder.AddUnbatching<T>() to integrate it properly. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.UnbatchingNodeNotSupported}";
    }

    public static string BatchingNodeNotSupported()
    {
        return $"[{ErrorCodes.BatchingNodeNotSupported}] BatchingNode doesn't support item-by-item transformation. " +
               $"BatchingNode accumulates items and processes them in batches. " +
               $"Use PipelineBuilder.AddBatching<T>() with appropriate configuration. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.BatchingNodeNotSupported}";
    }

    public static string CustomMergeNodeMissingInterface(string nodeTypeName)
    {
        return $"[{ErrorCodes.CustomMergeNodeMissingInterface}] Custom merge node '{nodeTypeName}' does not implement the expected generic interface. " +
               $"Custom merge nodes must implement ICusMergeNode<T1, T2, TOut>. " +
               $"Review your merge node implementation. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.CustomMergeNodeMissingInterface}";
    }

    public static string UnbatchingExecutionStrategyMissingDeadLetterHandler()
    {
        return $"[{ErrorCodes.UnbatchingExecutionStrategyMissingDeadLetterHandler}] Unbatching execution strategy could not find dead letter handler. " +
               $"Items that fail unbatching need a dead letter sink for error handling. " +
               $"Ensure a dead letter handler is configured in the pipeline context. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.UnbatchingExecutionStrategyMissingDeadLetterHandler}";
    }

    public static string LineageAdapterMissing(string nodeId)
    {
        return $"[{ErrorCodes.LineageAdapterMissing}] Lineage adapter missing for node '{nodeId}'. " +
               $"This is an internal framework error - the node wasn't properly registered. " +
               $"Please report this to the NPipeline project. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.LineageAdapterMissing}";
    }

    public static string SourceNodeLineageUnwrapMissing(string nodeId)
    {
        return $"[{ErrorCodes.SourceNodeLineageUnwrapMissing}] Source node lineage unwrap delegate missing for node '{nodeId}'. " +
               $"This is an internal framework error. " +
               $"Please report this to the NPipeline project. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.SourceNodeLineageUnwrapMissing}";
    }

    public static string SinkNodeLineageUnwrapMissing(string nodeId)
    {
        return $"[{ErrorCodes.SinkNodeLineageUnwrapMissing}] Sink node lineage unwrap delegate missing for node '{nodeId}'. " +
               $"This is an internal framework error. " +
               $"Please report this to the NPipeline project. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.SinkNodeLineageUnwrapMissing}";
    }

    public static string MissingTypeMetadata(string nodeId, string metadataName)
    {
        return $"[{ErrorCodes.MissingTypeMetadata}] Node '{nodeId}' missing {metadataName} metadata. " +
               $"This is an internal framework error during graph construction. " +
               $"Please report this to the NPipeline project. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.MissingTypeMetadata}";
    }

    public static string TimeWindowAssignerCannotBeNull()
    {
        return $"[{ErrorCodes.TimeWindowAssignerCannotBeNull}] Time window assigner cannot be null. " +
               $"Windowed nodes require a window assignment strategy. " +
               $"Provide a non-null window assigner in the constructor. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.TimeWindowAssignerCannotBeNull}";
    }

    #endregion

    #region Resource and Capacity Errors (NP05xx)

    public static string ContextDisposalFailed(int errorCount)
    {
        return $"[{ErrorCodes.ContextDisposalFailed}] One or more errors occurred disposing pipeline context resources ({errorCount} errors). " +
               $"Check the inner exceptions for details on resource disposal failures. " +
               $"Some resources may not have been properly cleaned up. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.ContextDisposalFailed}";
    }

    public static string DeadLetterQueueCapacityExceeded(int capacity)
    {
        return $"[{ErrorCodes.DeadLetterQueueCapacityExceeded}] Dead Letter Queue has exceeded its capacity of {capacity}. " +
               $"Too many failed items have been queued for the dead letter sink. " +
               $"Failing the pipeline to prevent memory overflow. Increase capacity or fix upstream errors. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.DeadLetterQueueCapacityExceeded}";
    }

    public static string MaterializationCapExceeded(string nodeId, int cap)
    {
        return $"[{ErrorCodes.MaterializationCapExceeded}] Materialization cap exceeded for node '{nodeId}' (cap={cap}). " +
               $"Too many items are being buffered in memory. " +
               $"Reduce the volume of items or increase the materialization cap. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.MaterializationCapExceeded}";
    }

    public static string BatchSizeMustBeGreaterThanZero()
    {
        return $"[{ErrorCodes.BatchSizeMustBeGreaterThanZero}] Batch size must be greater than zero. " +
               $"Provide a positive integer for the batch size parameter. " +
               $"See: {DocsBaseUrl}#{ErrorCodes.BatchSizeMustBeGreaterThanZero}";
    }

    #endregion
}
#pragma warning restore IDE0022
