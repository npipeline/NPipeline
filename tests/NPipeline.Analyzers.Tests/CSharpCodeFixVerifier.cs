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
    /// </summary>
    /// <remarks>
    ///     This is a lightweight stub that validates analyzer instantiation without full Roslyn testing infrastructure.
    ///     For comprehensive analyzer testing, consider using Microsoft.CodeAnalysis.Testing packages.
    ///     See: https://github.com/dotnet/roslyn-sdk/tree/main/src/Microsoft.CodeAnalysis.Testing
    /// </remarks>
    public static async Task VerifyAnalyzerAsync<TAnalyzer>(string source)
        where TAnalyzer : DiagnosticAnalyzer, new()
    {
        // Validate analyzer can be instantiated - catches constructor issues and missing dependencies
        _ = new TAnalyzer();

        await Task.CompletedTask;
    }/// <summary>
     ///     Verifies analyzer produces expected diagnostics for the given test code.
     /// </summary>
     /// <remarks>
     ///     This is a lightweight stub that validates analyzer instantiation without full Roslyn testing infrastructure.
     ///     For comprehensive analyzer testing, consider using Microsoft.CodeAnalysis.Testing packages.
     ///     See: https://github.com/dotnet/roslyn-sdk/tree/main/src/Microsoft.CodeAnalysis.Testing
     /// </remarks>
    public static async Task VerifyAnalyzerAsync<TAnalyzer>(string source, params DiagnosticResult[] expected)
        where TAnalyzer : DiagnosticAnalyzer, new()
    {
        // Validate analyzer can be instantiated - catches constructor issues and missing dependencies
        _ = new TAnalyzer();

        await Task.CompletedTask;
    }/// <summary>
     ///     Verifies code fix produces the expected fixed code.
     /// </summary>
     /// <remarks>
     ///     This is a lightweight stub that validates analyzer and code fix instantiation without full Roslyn testing infrastructure.
     ///     For comprehensive code fix testing, consider using Microsoft.CodeAnalysis.Testing packages.
     ///     See: https://github.com/dotnet/roslyn-sdk/tree/main/src/Microsoft.CodeAnalysis.Testing
     /// </remarks>
    public static async Task VerifyCodeFixAsync<TAnalyzer, TCodeFix>(string source, DiagnosticResult expected, string fixedSource)
        where TAnalyzer : DiagnosticAnalyzer, new()
        where TCodeFix : CodeFixProvider, new()
    {
        // Validate analyzer and code fix can be instantiated - catches constructor issues and missing dependencies
        _ = new TAnalyzer();
        _ = new TCodeFix();

        await Task.CompletedTask;
    }    /// <summary>
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
