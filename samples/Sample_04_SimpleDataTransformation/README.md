# Sample 4: Simple Data Transformation

## Overview

This sample demonstrates data manipulation and type transformation patterns through a CSV processing pipeline with validation, filtering, and enrichment.

## Core Concepts

1. **CSV to Object Transformation**
   - Reading CSV data and converting to strongly-typed objects
   - Using CsvHelper for robust CSV parsing

2. **Data Validation Patterns**
   - Business rule validation
   - Email format validation
   - Data quality checks

3. **Filtering Mechanisms**
   - Age-based filtering
   - Geographic filtering
   - Conditional data exclusion

4. **Data Enrichment**
   - Adding country information based on city
   - Age categorization
   - Email validation status

## Quick Setup and Run

### Prerequisites

- .NET 8.0, .NET 9.0 or .NET 10.0 SDK
- JetBrains Rider, Visual Studio 2022, VS Code, or .NET CLI

### Running the Sample

```bash
cd samples/Sample_04_SimpleDataTransformation
dotnet restore
dotnet run
```

## Pipeline Flow

1. **CsvSource** reads CSV data and converts to Person objects
2. **ValidationTransform** validates Person objects according to business rules
3. **FilteringTransform** filters Person objects based on age and city conditions
4. **EnrichmentTransform** adds country, age category, and email validation status
5. **ConsoleSink** outputs the final transformed data with summary statistics

## Expected Output

The pipeline will process 10 sample records, filter out invalid ones, and display the final enriched data with a summary showing validation and filtering results.