param(
    [Parameter(Mandatory=$false)]
    [ValidateSet("Debug", "Release")]
    [string]$Configuration = "Release",
    
    [Parameter(Mandatory=$false)]
    [switch]$Pack,
    
    [Parameter(Mandatory=$false)]
    [switch]$Help
)

# Function to display usage information
function Show-Help {
    Write-Host "NPipeline Build Script" -ForegroundColor Green
    Write-Host "======================" -ForegroundColor Green
    Write-Host ""
    Write-Host "Usage:" -ForegroundColor Yellow
    Write-Host "  .\build.ps1 [parameters]"
    Write-Host ""
    Write-Host "Parameters:" -ForegroundColor Yellow
    Write-Host "  -Configuration <Debug|Release>  Build configuration (default: Release)"
    Write-Host "  -Pack                          Create NuGet packages after build"
    Write-Host "  -Help                          Show this help message"
    Write-Host ""
    Write-Host "Examples:" -ForegroundColor Yellow
    Write-Host "  .\build.ps1"
    Write-Host "  .\build.ps1 -Configuration Debug"
    Write-Host "  .\build.ps1 -Pack"
    Write-Host "  .\build.ps1 -Configuration Release -Pack"
}

# Function to check if .NET SDK is installed
function Test-DotNetSdk {
    try {
        $dotnetVersion = dotnet --version
        Write-Host "Found .NET SDK version: $dotnetVersion" -ForegroundColor Green
        return $true
    }
    catch {
        Write-Host "Error: .NET SDK is not installed or not in PATH." -ForegroundColor Red
        Write-Host "Please install .NET SDK 10.0.100 or later from https://dotnet.microsoft.com/download" -ForegroundColor Red
        return $false
    }
}

# Function to create artifacts directory if needed
function Ensure-ArtifactsDirectory {
    if ($Pack -and -not (Test-Path "./artifacts")) {
        Write-Host "Creating artifacts directory..." -ForegroundColor Yellow
        New-Item -ItemType Directory -Path "./artifacts" | Out-Null
        Write-Host "Artifacts directory created." -ForegroundColor Green
    }
}

# Main execution
try {
    # Show help if requested
    if ($Help) {
        Show-Help
        exit 0
    }

    Write-Host "Starting NPipeline build..." -ForegroundColor Cyan
    Write-Host "Configuration: $Configuration" -ForegroundColor Cyan
    if ($Pack) {
        Write-Host "Package creation: Enabled" -ForegroundColor Cyan
    }
    Write-Host ""

    # Check if .NET SDK is installed
    if (-not (Test-DotNetSdk)) {
        exit 1
    }

    # Create artifacts directory if packing is enabled
    Ensure-ArtifactsDirectory

    Write-Host "Step 1: Restoring NuGet packages..." -ForegroundColor Yellow
    dotnet restore
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error: Package restore failed." -ForegroundColor Red
        exit $LASTEXITCODE
    }
    Write-Host "Package restore completed successfully." -ForegroundColor Green
    Write-Host ""

    Write-Host "Step 2: Building solution..." -ForegroundColor Yellow
    dotnet build --configuration $Configuration --no-restore
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error: Build failed." -ForegroundColor Red
        exit $LASTEXITCODE
    }
    Write-Host "Build completed successfully." -ForegroundColor Green
    Write-Host ""

    Write-Host "Step 3: Running tests..." -ForegroundColor Yellow
    dotnet test --configuration $Configuration --no-build --verbosity minimal
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Error: Tests failed." -ForegroundColor Red
        exit $LASTEXITCODE
    }
    Write-Host "All tests passed successfully." -ForegroundColor Green
    Write-Host ""

    if ($Pack) {
        Write-Host "Step 4: Creating NuGet packages..." -ForegroundColor Yellow
        dotnet pack --configuration $Configuration --no-build --output ./artifacts
        if ($LASTEXITCODE -ne 0) {
            Write-Host "Error: Package creation failed." -ForegroundColor Red
            exit $LASTEXITCODE
        }
        Write-Host "NuGet packages created successfully in ./artifacts directory." -ForegroundColor Green
        Write-Host ""
    }

    Write-Host "Build completed successfully!" -ForegroundColor Green
    if ($Pack) {
        Write-Host "Check the ./artifacts directory for generated packages." -ForegroundColor Cyan
    }
    exit 0
}
catch {
    Write-Host "Error: An unexpected error occurred during build: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}