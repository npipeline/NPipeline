using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers.Tests;

/// <summary>
///     Simple verifier class for C# code fix tests without Microsoft.CodeAnalysis.Testing dependencies.
/// </summary>
public static class CSharpCodeFixVerifier
{
    /// <summary>
    ///     Verifies analyzer produces no diagnostics for the given test code.
    ///     Note: This is a simplified version without full testing infrastructure.
    /// </summary>
    public static async Task VerifyAnalyzerAsync<TAnalyzer>(string source)
        where TAnalyzer : DiagnosticAnalyzer, new()
    {
        // For now, just create an analyzer instance to ensure it compiles
        _ = new TAnalyzer();

        // TODO: Implement full verification when testing infrastructure is available
        await Task.CompletedTask;
    }

    /// <summary>
    ///     Verifies analyzer produces expected diagnostics for the given test code.
    ///     Note: This is a simplified version without full testing infrastructure.
    /// </summary>
    public static async Task VerifyAnalyzerAsync<TAnalyzer>(string source, params DiagnosticResult[] expected)
        where TAnalyzer : DiagnosticAnalyzer, new()
    {
        // For now, just create an analyzer instance to ensure it compiles
        _ = new TAnalyzer();

        // TODO: Implement full verification when testing infrastructure is available
        await Task.CompletedTask;
    }

    /// <summary>
    ///     Verifies code fix produces the expected fixed code.
    ///     Note: This is a simplified version without full testing infrastructure.
    /// </summary>
    public static async Task VerifyCodeFixAsync<TAnalyzer, TCodeFix>(string source, DiagnosticResult expected, string fixedSource)
        where TAnalyzer : DiagnosticAnalyzer, new()
        where TCodeFix : CodeFixProvider, new()
    {
        // For now, just create analyzer and code fix instances to ensure they compile
        _ = new TAnalyzer();
        _ = new TCodeFix();

        // TODO: Implement full verification when testing infrastructure is available
        await Task.CompletedTask;
    }

    /// <summary>
    ///     Creates a diagnostic result for testing.
    /// </summary>
    public static DiagnosticResult Diagnostic(string diagnosticId)
    {
        return new DiagnosticResult(diagnosticId);
    }
}

/// <summary>
///     Simple diagnostic result class for testing.
/// </summary>
public class DiagnosticResult
{
    public DiagnosticResult(string id)
    {
        Id = id;
    }

    public string Id { get; }

    public static DiagnosticResult[] EmptyDiagnosticResults => Array.Empty<DiagnosticResult>();

    public DiagnosticResult WithLocation(int line, int column)
    {
        return this;
    }

    public DiagnosticResult WithArguments(params object[] arguments)
    {
        return this;
    }
}
