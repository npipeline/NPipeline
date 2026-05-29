using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Base class for analyzers that should only run when the HighThroughput optimization profile is active.
/// </summary>
public abstract class ProfileGatedDiagnosticAnalyzer : DiagnosticAnalyzer
{
    /// <inheritdoc />
    public sealed override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        context.RegisterCompilationStartAction(compilationStartContext =>
        {
            if (!AnalyzerProfileHelper.IsHighThroughput(compilationStartContext.Options, compilationStartContext.Compilation))
                return;

            RegisterProfileGatedActions(compilationStartContext);
        });
    }

    /// <summary>
    ///     Registers analyzer actions that should run only when profile gating passes.
    /// </summary>
    protected abstract void RegisterProfileGatedActions(CompilationStartAnalysisContext context);
}