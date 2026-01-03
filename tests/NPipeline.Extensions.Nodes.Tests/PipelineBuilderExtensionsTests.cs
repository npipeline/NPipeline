using AwesomeAssertions;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Nodes.Tests;

public sealed class PipelineBuilderExtensionsTests
{
    #region String Cleansing Tests

    [Fact]
    public void AddStringCleansing_BareVariant_ShouldRegisterNode()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddStringCleansing<TestModel>("stringcleansing");

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("stringcleansing");
    }

    [Fact]
    public void AddStringCleansing_WithConfiguration_ShouldRegisterPreconfiguredInstance()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddStringCleansing<TestModel>(n => n.Trim(x => x.Name), "stringclean");

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("stringclean");
    }

    [Fact]
    public void AddStringCleansing_WithNullConfigure_ShouldThrowArgumentNullException()
    {
        // Arrange
        var builder = new PipelineBuilder();
        Action<StringCleansingNode<TestModel>>? configure = null;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            builder.AddStringCleansing(configure!));
    }

    [Fact]
    public void AddStringCleansing_WithoutName_ShouldUseTypeName()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddStringCleansing<TestModel>();

        // Assert
        handle.Id.Should().Be("stringcleansingnode-1");
    }

    #endregion

    #region Numeric Cleansing Tests

    [Fact]
    public void AddNumericCleansing_BareVariant_ShouldRegisterNode()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddNumericCleansing<TestModel>("numericcleansing");

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("numericcleansing");
    }

    [Fact]
    public void AddNumericCleansing_WithConfiguration_ShouldRegisterPreconfiguredInstance()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddNumericCleansing<TestModel>(n => n.Clamp(x => x.Age, 0, 150), "numericclean");

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("numericclean");
    }

    [Fact]
    public void AddNumericCleansing_WithNullConfigure_ShouldThrowArgumentNullException()
    {
        // Arrange
        var builder = new PipelineBuilder();
        Action<NumericCleansingNode<TestModel>>? configure = null;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            builder.AddNumericCleansing(configure!));
    }

    #endregion

    #region DateTime Cleansing Tests

    [Fact]
    public void AddDateTimeCleansing_BareVariant_ShouldRegisterNode()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddDateTimeCleansing<TestModel>("datetimecleansing");

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("datetimecleansing");
    }

    [Fact]
    public void AddDateTimeCleansing_WithConfiguration_ShouldRegisterPreconfiguredInstance()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddDateTimeCleansing<TestModel>(n => n.ToUtc(x => x.CreatedDate), "datetimeclean");

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("datetimeclean");
    }

    [Fact]
    public void AddDateTimeCleansing_WithNullConfigure_ShouldThrowArgumentNullException()
    {
        // Arrange
        var builder = new PipelineBuilder();
        Action<DateTimeCleansingNode<TestModel>>? configure = null;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            builder.AddDateTimeCleansing(configure!));
    }

    #endregion

    #region Collection Cleansing Tests

    [Fact]
    public void AddCollectionCleansing_BareVariant_ShouldRegisterNode()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddCollectionCleansing<TestModel>("collectioncleansing");

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("collectioncleansing");
    }

    [Fact]
    public void AddCollectionCleansing_WithConfiguration_ShouldRegisterPreconfiguredInstance()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddCollectionCleansing<TestModel>(
            n => n.RemoveNulls(x => x.Tags!),
            "collectionclean");

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("collectionclean");
    }

    [Fact]
    public void AddCollectionCleansing_WithNullConfigure_ShouldThrowArgumentNullException()
    {
        // Arrange
        var builder = new PipelineBuilder();
        Action<CollectionCleansingNode<TestModel>>? configure = null;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            builder.AddCollectionCleansing(configure!));
    }

    #endregion

    #region String Validation Tests

    [Fact]
    public void AddStringValidation_BareVariant_ShouldRegisterNodeWithDefaultErrorHandler()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddStringValidation<TestModel>("stringvalidation");

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("stringvalidation");
    }

    [Fact]
    public void AddStringValidation_BareVariantWithoutName_ShouldUseTypeName()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddStringValidation<TestModel>();

        // Assert
        handle.Id.Should().Be("stringvalidationnode-1");
    }

    [Fact]
    public void AddStringValidation_BareVariantWithoutErrorHandler_ShouldNotRegisterDefaultHandler()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddStringValidation<TestModel>(applyDefaultErrorHandler: false);

        // Assert
        handle.Should().NotBeNull();
    }

    [Fact]
    public void AddStringValidation_WithConfiguration_ShouldRegisterNodeWithDefaultErrorHandler()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddStringValidation<TestModel>(n => n.HasMinLength(x => x.Name, 1));

        // Assert
        handle.Should().NotBeNull();
    }

    [Fact]
    public void AddStringValidation_WithNullConfigure_ShouldThrowArgumentNullException()
    {
        // Arrange
        var builder = new PipelineBuilder();
        Action<StringValidationNode<TestModel>>? configure = null;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            builder.AddStringValidation(configure!));
    }

    [Fact]
    public void AddStringValidation_WithoutErrorHandler_ShouldNotRegisterDefaultHandler()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddStringValidation<TestModel>(
            n => n.HasMinLength(x => x.Name, 1),
            applyDefaultErrorHandler: false);

        // Assert
        handle.Should().NotBeNull();
    }

    #endregion

    #region Numeric Validation Tests

    [Fact]
    public void AddNumericValidation_BareVariant_ShouldRegisterNodeWithDefaultErrorHandler()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddNumericValidation<TestModel>("numericvalidation");

        // Assert
        handle.Should().NotBeNull();
    }

    [Fact]
    public void AddNumericValidation_WithConfiguration_ShouldRegisterPreconfiguredInstance()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddNumericValidation<TestModel>(
            n => n.IsPositive(x => x.Age),
            "numericval");

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("numericval");
    }

    [Fact]
    public void AddNumericValidation_WithNullConfigure_ShouldThrowArgumentNullException()
    {
        // Arrange
        var builder = new PipelineBuilder();
        Action<NumericValidationNode<TestModel>>? configure = null;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            builder.AddNumericValidation(configure!));
    }

    #endregion

    #region DateTime Validation Tests

    [Fact]
    public void AddDateTimeValidation_BareVariant_ShouldRegisterNodeWithDefaultErrorHandler()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddDateTimeValidation<TestModel>("datetimevalidation");

        // Assert
        handle.Should().NotBeNull();
    }

    [Fact]
    public void AddDateTimeValidation_WithConfiguration_ShouldRegisterPreconfiguredInstance()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddDateTimeValidation<TestModel>(
            n => n.IsInPast(x => x.CreatedDate),
            "datetimeval");

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("datetimeval");
    }

    [Fact]
    public void AddDateTimeValidation_WithNullConfigure_ShouldThrowArgumentNullException()
    {
        // Arrange
        var builder = new PipelineBuilder();
        Action<DateTimeValidationNode<TestModel>>? configure = null;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            builder.AddDateTimeValidation(configure!));
    }

    #endregion

    #region Collection Validation Tests

    [Fact]
    public void AddCollectionValidation_BareVariant_ShouldRegisterNodeWithDefaultErrorHandler()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddCollectionValidation<TestModel>("collectionvalidation");

        // Assert
        handle.Should().NotBeNull();
    }

    [Fact]
    public void AddCollectionValidation_WithConfiguration_ShouldRegisterPreconfiguredInstance()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddCollectionValidation<TestModel>(
            n => n.HasMinCount(x => x.Tags, 1),
            "collectionval");

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("collectionval");
    }

    [Fact]
    public void AddCollectionValidation_WithNullConfigure_ShouldThrowArgumentNullException()
    {
        // Arrange
        var builder = new PipelineBuilder();
        Action<CollectionValidationNode<TestModel>>? configure = null;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            builder.AddCollectionValidation(configure!));
    }

    [Fact]
    public void AddCollectionValidation_WithoutErrorHandler_ShouldNotRegisterDefaultHandler()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddCollectionValidation<TestModel>(
            n => n.HasMinCount(x => x.Tags, 1),
            applyDefaultErrorHandler: false);

        // Assert
        handle.Should().NotBeNull();
    }

    #endregion

    #region Generic Validation Tests

    [Fact]
    public void AddValidationNode_BareVariant_ShouldRegisterNodeWithDefaultErrorHandler()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddValidationNode<string, TestValidationNode>("testvalidation");

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("testvalidation");
    }

    [Fact]
    public void AddValidationNode_WithConfiguration_ShouldRegisterPreconfiguredInstance()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddValidationNode<string, TestValidationNode>(
            configure: null,
            name: "testvariantnull");

        // Assert
        handle.Should().NotBeNull();
    }

    [Fact]
    public void AddValidationNode_WithoutName_ShouldUseTypeName()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddValidationNode<string, TestValidationNode>();

        // Assert
        handle.Id.Should().Be("testvalidationnode");
    }

    [Fact]
    public void AddValidationNode_WithoutErrorHandler_ShouldNotRegisterDefaultHandler()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddValidationNode<string, TestValidationNode>(
            applyDefaultErrorHandler: false);

        // Assert
        handle.Should().NotBeNull();
    }

    #endregion

    #region Filtering Tests

    [Fact]
    public void AddFilteringNode_BareVariant_ShouldRegisterNodeWithDefaultErrorHandler()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddFilteringNode<string>("testfiltering");

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("testfiltering");
    }

    [Fact]
    public void AddFilteringNode_WithConfiguration_ShouldRegisterPreconfiguredInstance()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddFilteringNode<TestModel>(
            n => n.Where(x => x.Age > 18),
            "filteringconfig");

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("filteringconfig");
    }

    [Fact]
    public void AddFilteringNode_WithNullConfigure_ShouldThrowArgumentNullException()
    {
        // Arrange
        var builder = new PipelineBuilder();
        Action<FilteringNode<TestModel>>? configure = null;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            builder.AddFilteringNode(configure!));
    }

    [Fact]
    public void AddFilteringNode_WithoutErrorHandler_ShouldNotRegisterDefaultHandler()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddFilteringNode<string>(
            applyDefaultErrorHandler: false);

        // Assert
        handle.Should().NotBeNull();
    }

    [Fact]
    public void AddFilteringNode_WithoutName_ShouldUseTypeName()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddFilteringNode<string>();

        // Assert
        handle.Id.Should().StartWith("filteringnode");
    }

    #endregion

    #region Type Conversion Tests

    [Fact]
    public void AddTypeConversion_BareVariant_ShouldRegisterNodeWithDefaultErrorHandler()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddTypeConversion<string, int>("conversion");

        // Assert
        handle.Should().NotBeNull();
    }

    [Fact]
    public void AddTypeConversion_WithDelegateConverter_ShouldRegisterPreconfiguredInstance()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddTypeConversion<string, int>(s => int.Parse(s), "stringtoint");

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("stringtoint");
    }

    [Fact]
    public void AddTypeConversion_WithNullConverter_ShouldThrowArgumentNullException()
    {
        // Arrange
        var builder = new PipelineBuilder();
        Func<string, int>? converter = null;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            builder.AddTypeConversion(converter!));
    }

    [Fact]
    public void AddTypeConversion_WithConfiguration_ShouldRegisterPreconfiguredInstance()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddTypeConversion<string, int>(
            n => n.WithConverter(s => int.Parse(s)),
            "configconv");

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("configconv");
    }

    [Fact]
    public void AddTypeConversion_WithNullConfigure_ShouldThrowArgumentNullException()
    {
        // Arrange
        var builder = new PipelineBuilder();
        Action<TypeConversionNode<string, int>>? configure = null;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            builder.AddTypeConversion(configure!));
    }

    [Fact]
    public void AddTypeConversion_WithoutErrorHandler_ShouldNotRegisterDefaultHandler()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddTypeConversion<string, int>(
            applyDefaultErrorHandler: false);

        // Assert
        handle.Should().NotBeNull();
    }

    #endregion

    #region Enrichment Tests

    [Fact]
    public void AddEnrichment_BareVariant_ShouldRegisterNode()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddEnrichment<TestModel>("enrichment");

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("enrichment");
    }

    [Fact]
    public void AddEnrichment_BareVariantWithoutName_ShouldUseTypeName()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddEnrichment<TestModel>();

        // Assert
        handle.Id.Should().StartWith("enrichmentnode");
    }

    [Fact]
    public void AddEnrichment_WithConfiguration_ShouldRegisterPreconfiguredInstance()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddEnrichment<TestModel>(
            n => n.DefaultIfNull(x => x.Name, "Unknown"),
            "enrichment");

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("enrichment");
    }

    [Fact]
    public void AddEnrichment_WithNullConfigure_ShouldThrowArgumentNullException()
    {
        // Arrange
        var builder = new PipelineBuilder();
        Action<EnrichmentNode<TestModel>>? configure = null;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            builder.AddEnrichment(configure!));
    }

    #endregion

    #region TransformationNode Tests

    [Fact]
    public void AddTransformationNode_BareVariant_ShouldRegisterNode()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddTransformationNode<TestModel, TestCustomTransformationNode>("customtransform");

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("customtransform");
    }

    [Fact]
    public void AddTransformationNode_WithoutName_ShouldUseTypeName()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddTransformationNode<TestModel, TestCustomTransformationNode>();

        // Assert
        handle.Id.Should().StartWith("testcustomtransformationnode");
    }

    [Fact]
    public void AddTransformationNode_WithConfiguration_ShouldRegisterPreconfiguredInstance()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddTransformationNode<TestModel, TestCustomTransformationNode>(
            configure: n => n.Register(x => x.Name, name => name?.ToUpperInvariant() ?? string.Empty),
            name: "customconfig");

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("customconfig");
    }

    #endregion

    #region Multiple Node Chaining Tests

    [Fact]
    public void MultipleNodes_ShouldAllowChaining()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var cleansing = builder.AddStringCleansing<TestModel>(n => n.Trim(x => x.Name), "clean");
        var validation = builder.AddStringValidation<TestModel>(n => n.HasMinLength(x => x.Name, 1), "val");
        var filtering = builder.AddFilteringNode<TestModel>(n => n.Where(x => x.Age > 0), "filter");

        // Assert
        cleansing.Should().NotBeNull();
        validation.Should().NotBeNull();
        filtering.Should().NotBeNull();
        cleansing.Id.Should().NotBe(validation.Id);
        validation.Id.Should().NotBe(filtering.Id);
    }

    [Fact]
    public void ComplexPipeline_ShouldBuildSuccessfully()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        builder.AddStringCleansing<TestModel>(n => n.Trim(x => x.Name));
        builder.AddNumericCleansing<TestModel>(n => n.Clamp(x => x.Age, 0, 150));
        builder.AddStringValidation<TestModel>(n => n.HasMinLength(x => x.Name, 1));
        builder.AddNumericValidation<TestModel>(n => n.IsPositive(x => x.Age));
        builder.AddFilteringNode<TestModel>(n => n.Where(x => x.Age > 18));
        builder.AddCollectionValidation<TestModel>(n => n.HasMinCount(x => x.Tags, 0));

        // Assert
        builder.Should().NotBeNull();
    }

    #endregion

    #region Error Handler Tests

    [Fact]
    public void ValidationNodes_WithApplyDefaultErrorHandler_ShouldRegisterHandler()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddNumericValidation<TestModel>(
            applyDefaultErrorHandler: true);

        // Assert
        handle.Should().NotBeNull();
    }

    [Fact]
    public void FilteringNodes_WithApplyDefaultErrorHandler_ShouldRegisterHandler()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddFilteringNode<TestModel>(
            applyDefaultErrorHandler: true);

        // Assert
        handle.Should().NotBeNull();
    }

    [Fact]
    public void TypeConversionNodes_WithApplyDefaultErrorHandler_ShouldRegisterHandler()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddTypeConversion<string, int>(
            applyDefaultErrorHandler: true);

        // Assert
        handle.Should().NotBeNull();
    }

    #endregion

    #region Null Builder Tests

    [Fact]
    public void AddStringCleansing_WithNullBuilder_ShouldThrowArgumentNullException()
    {
        // Arrange
        PipelineBuilder builder = null!;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            builder.AddStringCleansing<TestModel>());
    }

    [Fact]
    public void AddNumericCleansing_WithNullBuilder_ShouldThrowArgumentNullException()
    {
        // Arrange
        PipelineBuilder builder = null!;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            builder.AddNumericCleansing<TestModel>());
    }

    [Fact]
    public void AddValidationNode_WithNullBuilder_ShouldThrowArgumentNullException()
    {
        // Arrange
        PipelineBuilder builder = null!;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            builder.AddValidationNode<string, TestValidationNode>());
    }

    [Fact]
    public void AddFilteringNode_WithNullBuilder_ShouldThrowArgumentNullException()
    {
        // Arrange
        PipelineBuilder builder = null!;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            builder.AddFilteringNode<string>());
    }

    #endregion

    #region Test Classes

    private sealed class TestModel
    {
        public string Name { get; } = string.Empty;
        public int Age { get; set; }
        public DateTime CreatedDate { get; set; }
        public IEnumerable<string>? Tags { get; set; }
    }

    private sealed class TestValidationNode : ValidationNode<string>
    {
    }

    private sealed class TestCustomTransformationNode : PropertyTransformationNode<TestModel>
    {
    }

    #endregion
}
