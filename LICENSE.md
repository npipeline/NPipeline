# NPipeline Ecosystem Licensing

The NPipeline ecosystem uses a hybrid licensing architecture. This structure protects the sustainability of our advanced enterprise integrations while keeping the core execution framework completely free and open for the community.

Licensing is applied granularly at the project/directory level. Please review the breakdown below.

---

## 1. Permissive Open Source (MIT License)

The core execution framework, standard extensions, and abstract base classes are 100% free open-source software. You may use, modify, and distribute them in any personal, open-source, or commercial production environment without restriction.

The following directories are governed by the **MIT License**:

* `/src/NPipeline/` (Core Engine)
* `/src/NPipeline.Analyzers/` (Core Analyzers)
* `/src/NPipeline.Analyzers.Package/` (Analyzer Package)
* `/src/NPipeline.CodeFixes/` (Code Fixes)
* `/src/NPipeline.Connectors/` (Base Abstractions)
* `/src/NPipeline.Extensions.DependencyInjection/`
* `/src/NPipeline.Extensions.Nodes/`
* `/src/NPipeline.Extensions.Testing/`
* `/src/NPipeline.StorageProviders/` (Base Abstractions)

*For the full legal text, see the `LICENSE.txt` file within any of the directories listed above.*

---

## 2. Fair Source / Commercial (Business Source License 1.1)

Our concrete production extensions, cloud database adapters, and advanced storage providers are source-available. They are free for non-production use, but production usage is tiered by organization size to ensure fair commercial use.

The following directories are governed by the **Business Source License 1.1 (BSL 1.1)**:

**Connectors**

* `/src/NPipeline.Connectors.Aws.Sqs/`
* `/src/NPipeline.Connectors.Azure/`
* `/src/NPipeline.Connectors.Azure.CosmosDb/`
* `/src/NPipeline.Connectors.Azure.ServiceBus/`
* `/src/NPipeline.Connectors.Csv/`
* `/src/NPipeline.Connectors.DataLake/`
* `/src/NPipeline.Connectors.DuckDB/`
* `/src/NPipeline.Connectors.Excel/`
* `/src/NPipeline.Connectors.Http/`
* `/src/NPipeline.Connectors.Json/`
* `/src/NPipeline.Connectors.Kafka/`
* `/src/NPipeline.Connectors.MongoDB/`
* `/src/NPipeline.Connectors.MySQL/`
* `/src/NPipeline.Connectors.Parquet/`
* `/src/NPipeline.Connectors.Postgres/`
* `/src/NPipeline.Connectors.Postgres.Analyzers/`
* `/src/NPipeline.Connectors.RabbitMQ/`
* `/src/NPipeline.Connectors.Snowflake/`
* `/src/NPipeline.Connectors.SqlServer/`
* `/src/NPipeline.Connectors.SqlServer.Analyzers/`

**Extensions**

* `/src/NPipeline.Extensions.AI/`
* `/src/NPipeline.Extensions.Composition/`
* `/src/NPipeline.Extensions.Lineage/`
* `/src/NPipeline.Extensions.Observability/`
* `/src/NPipeline.Extensions.Observability.OpenTelemetry/`
* `/src/NPipeline.Extensions.Parallelism/`
* `/src/NPipeline.Extensions.Testing.AwesomeAssertions/`
* `/src/NPipeline.Extensions.Testing.FluentAssertions/`

**Storage Providers**

* `/src/NPipeline.StorageProviders.Adls/`
* `/src/NPipeline.StorageProviders.Azure/`
* `/src/NPipeline.StorageProviders.Gcp/`
* `/src/NPipeline.StorageProviders.S3/`
* `/src/NPipeline.StorageProviders.S3.Aws/`
* `/src/NPipeline.StorageProviders.S3.Compatible/`
* `/src/NPipeline.StorageProviders.Sftp/`

* (And any other concrete Extension, Connector or Storage Provider package containing a `LICENSE.txt` file)*

### The BSL 1.1 Summary Terms

* **Non-Production Use (Free):** You may use these packages freely on localhost, in isolated development environments, and within CI/CD testing pipelines.
* **Production Use (Tiered):**
  * **Free Tier:** Production deployment is fully permitted and free of charge if your organization employs **four (4) or fewer software developers** across all teams AND has a gross annual revenue of **$5,000,000 AUD or less**.
  * **Commercial Tier:** If your organization exceeds either of these thresholds, utilizing these packages in a live production environment requires an active commercial agreement.
* **Delayed Open Source (The 2-Year Rule):** Exactly two (2) years after a specific version is released, its license automatically and permanently converts to the fully permissive **MIT License**.

*For the legally binding terms, exact conversion dates, and the governing law, see the `LICENSE.txt` file inside the respective project directory.*

---

## Commercial Licensing & Support

If your organization requires a commercial license to use the BSL-governed packages in production, or if you require an enterprise support SLA, please visit [npipeline.com](https://www.npipeline.com) to request or purchase an agreement. For open-source documentation and community resources, visit [npipeline.net](https://www.npipeline.net).
