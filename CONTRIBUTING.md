# Contributing to NPipeline

Thank you for your interest in contributing to NPipeline! This guide will help you get started with contributing to the project.

## Table of Contents

- [Development Setup](#development-setup)
- [Build and Test Commands](#build-and-test-commands)
- [Code Style and Conventions](#code-style-and-conventions)
- [Pull Request Process](#pull-request-process)
- [Testing Requirements](#testing-requirements)
- [Documentation](#documentation)
- [CI NuGet Publishing](#ci-nuget-publishing)
- [Release Process](#release-process)

## Development Setup

### Prerequisites

- **.NET SDK 10.0.100** or later (with rollForward set to latestFeature)
- **Git** for version control
- **JetBrains Rider**, **Visual Studio 2022** or **Visual Studio Code** with C# extension

### Getting Started

1. **Fork the repository** on GitHub and clone your fork locally:

   ```bash
   git clone https://github.com/your-username/NPipeline.git
   cd NPipeline
   ```

2. **Add the upstream remote** to keep your fork up-to-date:

   ```bash
   git remote add upstream https://github.com/NPipeline/NPipeline.git
   ```

3. **Build the solution** using the build script (recommended method):

   ```bash
   # On Mac/Linux
   ./build.sh
   
   # On Windows
   .\build.ps1
   ```
   
   This will restore packages, build the solution, and run tests automatically.

4. **Alternative: Manual setup** (if you prefer not to use the build scripts):

   ```bash
   # Restore NuGet packages
   dotnet restore
   
   # Build the solution to verify everything is working
   dotnet build --configuration Release
   
   # Run tests to ensure all tests pass
   dotnet test --configuration Release --no-build
   ```

## Build and Test Commands

### Using Build Scripts (Recommended)

The project provides standardized build scripts for a consistent development experience across platforms:

#### Mac/Linux (build.sh)

```bash
# Basic build (Release configuration)
./build.sh

# Debug build
./build.sh -c Debug

# Build with packaging
./build.sh -p

# Run tests only
./build.sh -t

# Clean, restore, build, and test
./build.sh -f

# Get help with all available options
./build.sh -h
```

#### Windows (build.ps1)

```powershell
# Basic build (Release configuration)
.\build.ps1

# Debug build
.\build.ps1 -Configuration Debug

# Build with packaging
.\build.ps1 -Pack

# Run tests only
.\build.ps1 -Test

# Clean, restore, build, and test
.\build.ps1 -Full

# Get help with all available options
.\build.ps1 -Help
```

### Manual Commands

If you prefer to use dotnet commands directly, or need more granular control:

#### Building the Solution

```bash
# Build the entire solution in Release mode
dotnet build --configuration Release

# Build a specific project
dotnet build src/NPipeline/NPipeline.csproj --configuration Release

# Build with detailed output
dotnet build --configuration Release --verbosity normal
```

#### Running Tests

```bash
# Run all tests
dotnet test --configuration Release

# Run tests for a specific project
dotnet test tests/NPipeline.Tests/NPipeline.Tests.csproj --configuration Release

# Run tests with detailed output
dotnet test --configuration Release --verbosity normal

# Run tests with code coverage
dotnet test --configuration Release --collect:"XPlat Code Coverage"
```

#### Packaging

```bash
# Create NuGet packages
dotnet pack --configuration Release --output ./artifacts

# Create packages for a specific project
dotnet pack src/NPipeline/NPipeline.csproj --configuration Release --output ./artifacts
```

## Code Style and Conventions

### Coding Standards

NPipeline follows the coding standards defined in [`.editorconfig`](.editorconfig). Key conventions include:

- **C# 12.0** language features
- **Nullable reference types** enabled throughout
- **Implicit usings** enabled
- **Treat warnings as errors** is enforced
- **Var** keyword preferred for local variables when type is apparent

### Code Organization

- Follow the existing project structure and naming conventions
- Use meaningful names for classes, methods, and variables
- Keep classes and methods focused on a single responsibility
- Add XML documentation for all public APIs

### Documentation

All public APIs must include XML documentation comments:

```csharp
/// <summary>
/// Brief description of what the method does.
/// </summary>
/// <param name="parameterName">Description of the parameter.</param>
/// <returns>Description of the return value.</returns>
/// <exception cref="ExceptionType">Description of when this exception is thrown.</exception>
public ReturnType MethodName(ParameterType parameterName)
{
    // Implementation
}
```

### Analyzers

The project includes custom Roslyn analyzers (`NPipeline.Analyzers`) that provide compile-time validation and guidance. Ensure your code passes all analyzer rules before submitting a pull request.

## Pull Request Process

### Branching Strategy

1. Create a new branch from the `main` branch for your feature or bugfix:

   ```bash
   git checkout -b feature/your-feature-name
   # or
   git checkout -b fix/your-bugfix-name
   ```

2. Make your changes and commit them with clear, descriptive messages:

   ```bash
   git add .
   git commit -m "feat: Add new feature description"
   # or
   git commit -m "fix: Resolve issue with component X"
   ```

3. Push your branch to your fork:

   ```bash
   git push origin feature/your-feature-name
   ```

4. Create a pull request to the `main` branch of the upstream repository.

### Pull Request Requirements

- **Title**: Use a clear, descriptive title with one of these prefixes:
  - `feat:` for new features
  - `fix:` for bug fixes
  - `docs:` for documentation changes
  - `style:` for code style changes (no functional changes)
  - `refactor:` for refactoring
  - `test:` for adding or modifying tests
  - `chore:` for maintenance tasks

- **Description**: Provide a clear description of:
  - What the change does
  - Why the change is necessary
  - How you tested the change
  - Any breaking changes (if applicable)

- **Testing**: Ensure all tests pass and add new tests for new functionality

- **Documentation**: Update documentation for any API changes

### Code Review Process

1. All pull requests require at least one approval from a maintainer
2. Address all review comments before the PR can be merged
3. Keep the PR focused on a single feature or fix
4. Ensure the PR is up-to-date with the main branch before merging

## Testing Requirements

### Test Framework

NPipeline uses **xUnit** as the primary testing framework, along with:
- **FluentAssertions** for readable assertions
- **AwesomeAssertions** for additional assertion capabilities
- **FakeItEasy** for mocking
- **Coverlet** for code coverage

### Test Structure

- Unit tests are located in the `tests/` directory
- Each project has corresponding test projects (e.g., `NPipeline.Tests` for `NPipeline`)
- Test projects follow the naming convention: `[ProjectName].Tests`

### Writing Tests

1. **Arrange, Act, Assert** pattern:
   ```csharp
   [Fact]
   public void Method_Should_ExpectedBehavior_When_Condition()
   {
       // Arrange
       var input = CreateTestInput();
       var expected = CreateExpectedResult();
       
       // Act
       var actual = SystemUnderTest.Method(input);
       
       // Assert
       actual.Should().Be(expected);
   }
   ```

2. **Test naming**: Use descriptive names that follow the pattern:
   `MethodName_Should_ExpectedBehavior_When_Condition`

3. **Test organization**: Group related tests in a class with a descriptive name

4. **Mocking**: Use FakeItEasy for creating test doubles when needed

### Test Coverage

- Aim for high test coverage for new code
- Ensure critical paths and edge cases are tested
- Add integration tests for complex scenarios

## Documentation

### API Documentation

- All public APIs must have XML documentation
- Include examples for complex APIs
- Use `<see cref="...">` to reference related types or members

### Project Documentation

- Update relevant documentation in the `docs/` directory
- Add new documentation for new features
- Ensure all examples in documentation are up-to-date and tested

## CI NuGet Publishing

NPipeline uses automated workflows to publish NuGet packages:

### Main Branch Publishing (Pre-release)

Every successful build on the `main` branch automatically publishes pre-release packages to NuGet.org:

- **Version Format**: `1.0.0-ci.YYYYMMDD.BUILD_NUMBER`
- **Trigger**: Automatic on successful `main` branch builds
- **Authentication**: Uses OpenID Connect (OIDC) for secure, tokenless authentication
- **Purpose**: Allows users to test the latest features and changes immediately

When your pull request is merged to `main`, your changes will be included in the next CI build and published as a pre-release package. This enables rapid feedback and testing of new features.

### Stable Releases

Official stable releases are managed by maintainers through the existing release workflow:

- Created via tagged commits
- Follow semantic versioning
- Published as stable packages to NuGet.org

For detailed setup instructions and configuration details, see [NuGet Publishing Setup](docs/deployment/nuget-publishing-setup.md).

## Release Process

Releases are managed by the maintainers following these steps:

1. Update version in `Directory.Build.props`
2. Update `CHANGELOG.md` with release notes
3. Create a Git tag with the version number
4. Build and publish NuGet packages
5. Create a GitHub release with the changelog

## Getting Help

If you need help with contributing:

- Check the [documentation](docs/) for guidance
- Look at existing code for examples
- Ask questions in the issue tracker
- Join discussions in GitHub Discussions

## Code of Conduct

Please note that this project adheres to a [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

Thank you for contributing to NPipeline!