namespace NPipeline;

/// <summary>
///     Well-known error codes for NPipeline exceptions.
///     Each error code is organized by category with a link to documentation.
///     Use these constants for error handling, logging, and documentation lookup.
/// </summary>
/// <remarks>
///     Error Code Organization:
///     Runtime Errors:
///     - NP01xx: Core Pipeline Errors
///     - NP02xx: Type System Errors
///     - NP03xx: Node Execution Errors
///     - NP04xx: Configuration and Setup Errors
///     - NP05xx: Resource Management Errors
///     - NP06xx: Data Flow Errors
///     - NP07xx: Lineage and Tracking Errors
///     - NP08xx: Extension and Integration Errors
///     Analyzer Diagnostics:
///     - NP90XX: Configuration &amp; Setup Analyzers
///     - NP91XX: Performance &amp; Optimization Analyzers
///     - NP92XX: Reliability &amp; Error Handling Analyzers
///     - NP93XX: Data Integrity &amp; Correctness Analyzers
///     - NP94XX: Design &amp; Architecture Analyzers
///     - NP99xx: Internal Framework Errors
///     Documentation: https://github.com/npipeline/NPipeline/docs/reference/error-codes
/// </remarks>
public static class ErrorCodes
{
    #region Configuration & Setup Analyzers (NP90XX)

    /// <summary>Detects missing resilience configuration that can cause runtime failures when RestartNode is returned.</summary>
    public const string IncompleteResilientConfiguration = "NP9001";

    /// <summary>Prevents unbounded memory growth in retry options.</summary>
    public const string UnboundedMaterializationConfiguration = "NP9002";

    /// <summary>Detects inappropriate parallelism configuration.</summary>
    public const string InappropriateParallelismConfiguration = "NP9003";

    /// <summary>Detects batching configuration mismatches.</summary>
    public const string BatchingConfigurationMismatch = "NP9004";

    /// <summary>Detects timeout configuration issues.</summary>
    public const string TimeoutConfiguration = "NP9005";

    #endregion

    #region Performance & Optimization Analyzers (NP91XX)

    /// <summary>Detects blocking operations in async methods.</summary>
    public const string BlockingAsyncOperation = "NP9101";

    /// <summary>Synchronous method calling async operations without proper await.</summary>
    public const string SynchronousOverAsync = "NP9102";

    /// <summary>Detects LINQ usage in performance-critical paths.</summary>
    public const string LinqInHotPaths = "NP9103";

    /// <summary>Detects inefficient string operations.</summary>
    public const string InefficientStringOperations = "NP9104";

    /// <summary>Detects unnecessary object allocations.</summary>
    public const string AnonymousObjectAllocation = "NP9105";

    /// <summary>Detects ValueTask operations that could be optimized by using Task instead.</summary>
    public const string MissingValueTaskOptimization = "NP9106";

    /// <summary>Source node is not streaming data properly.</summary>
    public const string SourceNodeNotStreamingData = "NP9107";

    /// <summary>Add parameterless constructor for better performance.</summary>
    public const string ParameterlessConstructorPerformanceSuggestion = "NP9108";

    #endregion

    #region Reliability & Error Handling Analyzers (NP92XX)

    /// <summary>Detects catch blocks that may swallow OperationCanceledException without re-throwing it.</summary>
    public const string SwallowedOperationCanceledException = "NP9201";

    /// <summary>Detects inefficient exception handling patterns.</summary>
    public const string InefficientExceptionHandling = "NP9202";

    /// <summary>Detects when methods don't properly respect cancellation tokens.</summary>
    public const string NodeNotRespectingCancellationToken = "NP9203";

    #endregion

    #region Data Integrity & Correctness Analyzers (NP93XX)

    /// <summary>SinkNode implementations that don't consume their input parameter, which can lead to data loss and unexpected pipeline failures.</summary>
    public const string SinkNodeInputNotConsumed = "NP9301";

    /// <summary>Detects unsafe access patterns on PipelineContext properties and dictionaries that can lead to NullReferenceException at runtime.</summary>
    public const string UnsafePipelineContextAccess = "NP9302";

    #endregion

    #region Design & Architecture Analyzers (NP94XX)

    /// <summary>Suggests using StreamTransformNode for streaming data processing.</summary>
    public const string StreamTransformNodeSuggestion = "NP9401";

    /// <summary>Detects StreamTransformNode with incompatible execution strategies.</summary>
    public const string StreamTransformNodeExecutionStrategy = "NP9402";

    /// <summary>Node missing public parameterless constructor.</summary>
    public const string MissingParameterlessConstructor = "NP9403";

    /// <summary>Detects dependency injection anti-patterns in node implementations.</summary>
    public const string DependencyInjectionAntiPattern = "NP9404";

    #endregion

    #region Core Pipeline Errors (NP01xx)

    /// <summary>A pipeline must have at least one node.</summary>
    public const string PipelineRequiresAtLeastOneNode = "NP0101";

    /// <summary>A node is missing a required input connection.</summary>
    public const string NodeMissingInputConnection = "NP0102";

    /// <summary>Cyclic dependency detected in pipeline graph.</summary>
    public const string CyclicDependencyDetected = "NP0103";

    /// <summary>A node with the given ID has already been added.</summary>
    public const string NodeAlreadyAdded = "NP0104";

    /// <summary>A node with the given name already exists.</summary>
    public const string NodeNameNotUnique = "NP0105";

    #endregion

    #region Type System Errors (NP02xx)

    /// <summary>Type mismatch in connection between nodes.</summary>
    public const string TypeMismatchInConnection = "NP0201";

    /// <summary>Input data pipe is not of the expected type.</summary>
    public const string InputDataPipeWrongType = "NP0202";

    /// <summary>Cannot register mappings after execution has begun.</summary>
    public const string CannotRegisterMappingsAfterExecution = "NP0203";

    /// <summary>Record type has no public constructors.</summary>
    public const string RecordTypeHasNoPublicConstructor = "NP0204";

    /// <summary>Invalid member access expression in selector.</summary>
    public const string InvalidMemberAccessExpression = "NP0205";

    /// <summary>Member is not writable (readonly or no setter).</summary>
    public const string MemberNotWritable = "NP0206";

    /// <summary>Failed to create setter for member.</summary>
    public const string SetterCreationFailed = "NP0207";

    /// <summary>ValueTuple constructor not found.</summary>
    public const string ValueTupleConstructorNotFound = "NP0208";

    /// <summary>Cannot concatenate streams due to type mismatch.</summary>
    public const string CannotConcatenateStreamsTypeMismatch = "NP0210";

    #endregion

    #region Node Execution Errors (NP03xx)

    /// <summary>Node kind is not supported or its execution delegate is missing.</summary>
    public const string NodeKindNotSupported = "NP0301";

    /// <summary>Could not find output for source node.</summary>
    public const string OutputNotFoundForSourceNode = "NP0302";

    /// <summary>Pipeline execution failed at a specific node.</summary>
    public const string PipelineExecutionFailedAtNode = "NP0303";

    /// <summary>Pipeline execution failed overall.</summary>
    public const string PipelineExecutionFailed = "NP0304";

    /// <summary>An item failed to process after maximum retries.</summary>
    public const string ItemFailedAfterMaxRetries = "NP0305";

    /// <summary>Error handling itself failed.</summary>
    public const string ErrorHandlingFailed = "NP0306";

    /// <summary>Lineage cardinality mismatch (inputs/outputs count mismatch).</summary>
    public const string LineageCardinalityMismatch = "NP0307";

    /// <summary>Failed to extract items from InMemoryDataPipe.</summary>
    public const string FailedToExtractItemsFromInMemoryDataPipe = "NP0308";

    /// <summary>Circuit breaker tripped after threshold of consecutive failures.</summary>
    public const string CircuitBreakerTripped = "NP0310";

    /// <summary>Retry limit exhausted after maximum attempts.</summary>
    public const string RetryLimitExhausted = "NP0311";

    #endregion

    #region Configuration and Setup Errors (NP04xx)

    /// <summary>Execution strategy can only be set for transform nodes.</summary>
    public const string ExecutionStrategyCannotBeSetForNonTransformNode = "NP0401";

    /// <summary>Node not found in builder for the requested operation.</summary>
    public const string NodeNotFoundInBuilder = "NP0402";

    /// <summary>Resilience can only be applied to transform nodes.</summary>
    public const string ResilienceCannotBeAppliedToNonTransformNode = "NP0403";

    /// <summary>Invalid error handler type (doesn't implement required interface).</summary>
    public const string InvalidErrorHandlerType = "NP0404";

    /// <summary>Pre-configured instance for node has already been added.</summary>
    public const string PreConfiguredInstanceAlreadyAdded = "NP0405";

    /// <summary>Pre-configured instance node not found in builder.</summary>
    public const string PreConfiguredInstanceNodeNotFound = "NP0406";

    /// <summary>Merge strategy is not supported.</summary>
    public const string MergeStrategyNotSupported = "NP0407";

    /// <summary>Cannot instantiate node (activation failed).</summary>
    public const string NodeActivationFailed = "NP0408";

    /// <summary>Join node requires exactly two KeySelectorAttribute declarations.</summary>
    public const string JoinNodeRequiresTwoKeySelectorAttributes = "NP0411";

    /// <summary>UnbatchingNode cannot be executed directly.</summary>
    public const string UnbatchingNodeNotSupported = "NP0412";

    /// <summary>BatchingNode doesn't support item-by-item transformation.</summary>
    public const string BatchingNodeNotSupported = "NP0413";

    /// <summary>Custom merge node missing required interface implementation.</summary>
    public const string CustomMergeNodeMissingInterface = "NP0414";

    /// <summary>Unbatching execution strategy missing dead letter handler.</summary>
    public const string UnbatchingExecutionStrategyMissingDeadLetterHandler = "NP0415";

    /// <summary>Lineage adapter missing (internal framework error).</summary>
    public const string LineageAdapterMissing = "NP0416";

    /// <summary>Source node lineage unwrap missing (internal framework error).</summary>
    public const string SourceNodeLineageUnwrapMissing = "NP0417";

    /// <summary>Sink node lineage unwrap missing (internal framework error).</summary>
    public const string SinkNodeLineageUnwrapMissing = "NP0418";

    /// <summary>Node missing type metadata (internal framework error).</summary>
    public const string MissingTypeMetadata = "NP0419";

    /// <summary>Time window assigner cannot be null.</summary>
    public const string TimeWindowAssignerCannotBeNull = "NP0420";

    #endregion

    #region Resource Management Errors (NP05xx)

    /// <summary>One or more errors occurred disposing pipeline context resources.</summary>
    public const string ContextDisposalFailed = "NP0501";

    /// <summary>Dead letter queue has exceeded capacity.</summary>
    public const string DeadLetterQueueCapacityExceeded = "NP0502";

    /// <summary>Materialization cap exceeded for node.</summary>
    public const string MaterializationCapExceeded = "NP0503";

    /// <summary>Batch size must be greater than zero.</summary>
    public const string BatchSizeMustBeGreaterThanZero = "NP0504";

    #endregion
}
