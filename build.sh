#!/bin/bash

# NPipeline Build Script for Mac/Linux
# Usage: ./build.sh [options]

# Default values
CONFIGURATION="Release"
PACK=false
HELP=false

# Function to display usage information
show_help() {
    echo "NPipeline Build Script"
    echo "======================"
    echo ""
    echo "Usage:"
    echo "  ./build.sh [options]"
    echo ""
    echo "Options:"
    echo "  -c, --configuration <Debug|Release>  Build configuration (default: Release)"
    echo "  -p, --pack                          Create NuGet packages after build"
    echo "  -h, --help                          Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./build.sh"
    echo "  ./build.sh --configuration Debug"
    echo "  ./build.sh --pack"
    echo "  ./build.sh -c Release -p"
}

# Function to check if .NET SDK is installed
check_dotnet_sdk() {
    if ! command -v dotnet &> /dev/null; then
        echo "Error: .NET SDK is not installed or not in PATH." >&2
        echo "Please install .NET SDK 10.0.100 or later from https://dotnet.microsoft.com/download" >&2
        return 1
    fi
    
    local dotnet_version=$(dotnet --version)
    echo "Found .NET SDK version: $dotnet_version"
    return 0
}

# Function to create artifacts directory if needed
ensure_artifacts_directory() {
    if [ "$PACK" = true ] && [ ! -d "./artifacts" ]; then
        echo "Creating artifacts directory..."
        mkdir -p "./artifacts"
        echo "Artifacts directory created."
    fi
}

# Function to handle errors
handle_error() {
    local exit_code=$1
    local step=$2
    echo "Error: $step failed." >&2
    exit $exit_code
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--configuration)
            CONFIGURATION="$2"
            if [[ "$CONFIGURATION" != "Debug" && "$CONFIGURATION" != "Release" ]]; then
                echo "Error: Configuration must be either Debug or Release." >&2
                exit 1
            fi
            shift 2
            ;;
        -p|--pack)
            PACK=true
            shift
            ;;
        -h|--help)
            HELP=true
            shift
            ;;
        *)
            echo "Error: Unknown option $1" >&2
            echo "Use --help for usage information." >&2
            exit 1
            ;;
    esac
done

# Main execution
main() {
    # Show help if requested
    if [ "$HELP" = true ]; then
        show_help
        exit 0
    fi

    echo "Starting NPipeline build..."
    echo "Configuration: $CONFIGURATION"
    if [ "$PACK" = true ]; then
        echo "Package creation: Enabled"
    fi
    echo ""

    # Check if .NET SDK is installed
    if ! check_dotnet_sdk; then
        exit 1
    fi

    # Create artifacts directory if packing is enabled
    ensure_artifacts_directory

    # Step 1: Restore NuGet packages
    echo "Step 1: Restoring NuGet packages..."
    if ! dotnet restore; then
        handle_error $? "Package restore"
    fi
    echo "Package restore completed successfully."
    echo ""

    # Step 2: Build solution
    echo "Step 2: Building solution..."
    if ! dotnet build --configuration "$CONFIGURATION" --no-restore; then
        handle_error $? "Build"
    fi
    echo "Build completed successfully."
    echo ""

    # Step 3: Run tests
    echo "Step 3: Running tests..."
    if ! dotnet test --configuration "$CONFIGURATION" --no-build --verbosity minimal; then
        handle_error $? "Tests"
    fi
    echo "All tests passed successfully."
    echo ""

    # Step 4: Create NuGet packages if requested
    if [ "$PACK" = true ]; then
        echo "Step 4: Creating NuGet packages..."
        if ! dotnet pack --configuration "$CONFIGURATION" --no-build --output ./artifacts; then
            handle_error $? "Package creation"
        fi
        echo "NuGet packages created successfully in ./artifacts directory."
        echo ""
    fi

    echo "Build completed successfully!"
    if [ "$PACK" = true ]; then
        echo "Check the ./artifacts directory for generated packages."
    fi
}

# Set exit on error
set -e

# Run main function
main