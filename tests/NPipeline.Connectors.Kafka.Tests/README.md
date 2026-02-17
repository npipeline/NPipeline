# NPipeline Kafka Connector Tests

## Unit Tests

The unit tests in this project run without any external dependencies. They use mock objects and in-memory implementations to test the connector logic.

```bash
dotnet test tests/NPipeline.Connectors.Kafka.Tests/
```

## Integration Tests

Integration tests require a running Kafka instance. These tests are marked with `[SkipIfNoKafka]` or are in separate test classes that check for Kafka
availability.

### Running Integration Tests Locally

#### Option 1: Docker Compose (Recommended)

Create a `docker-compose.yml` file in the project root:

```yaml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "9094:9094"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0

  schema-registry:
    image: confluentinc/cp-schema-registry:7.5.0
    depends_on:
      - kafka
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: 'kafka:9092'
```

Start the infrastructure:

```bash
docker-compose up -d
```

Wait for Kafka to be ready (typically 30-60 seconds), then run the integration tests:

```bash
dotnet test tests/NPipeline.Connectors.Kafka.Tests/ --filter "Category=Integration"
```

#### Option 2: Local Kafka Installation

If you have Kafka installed locally:

1. Set the `KAFKA_BOOTSTRAP_SERVERS` environment variable:

   ```bash
   export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   ```

2. Set the `SCHEMA_REGISTRY_URL` environment variable (if using Schema Registry):

   ```bash
   export SCHEMA_REGISTRY_URL=http://localhost:8081
   ```

3. Run the tests:

   ```bash
   dotnet test tests/NPipeline.Connectors.Kafka.Tests/
   ```

### CI/CD Integration

For CI/CD pipelines, use a test matrix that includes:

1. **Unit Tests Only** - Runs on every PR/commit
2. **Integration Tests** - Runs on merge to main/develop branches with Kafka service

#### GitHub Actions Example

```yaml
name: Kafka Connector Tests

on:
  push:
    branches: [main, develop]
  pull_request:

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Setup .NET
        uses: actions/setup-dotnet@v4
        with:
          dotnet-version: '8.0.x'
      - name: Run Unit Tests
        run: dotnet test tests/NPipeline.Connectors.Kafka.Tests/ --filter "Category!=Integration"

  integration-tests:
    runs-on: ubuntu-latest
    if: github.event_name == 'push'
    services:
      zookeeper:
        image: confluentinc/cp-zookeeper:7.5.0
        env:
          ZOOKEEPER_CLIENT_PORT: 2181
        ports:
          - 2181:2181
      kafka:
        image: confluentinc/cp-kafka:7.5.0
        env:
          KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
          KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
          KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
        ports:
          - 9092:9092
      schema-registry:
        image: confluentinc/cp-schema-registry:7.5.0
        env:
          SCHEMA_REGISTRY_HOST_NAME: schema-registry
          SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:9092
        ports:
          - 8081:8081
    steps:
      - uses: actions/checkout@v4
      - name: Setup .NET
        uses: actions/setup-dotnet@v4
        with:
          dotnet-version: '8.0.x'
      - name: Wait for Kafka
        run: sleep 30
      - name: Run Integration Tests
        run: dotnet test tests/NPipeline.Connectors.Kafka.Tests/ --filter "Category=Integration"
```

## Test Categories

| Category    | Description           | Requires Kafka |
|-------------|-----------------------|----------------|
| Unit        | Fast, isolated tests  | No             |
| Integration | Tests with real Kafka | Yes            |

## Environment Variables

| Variable                  | Description            | Default                 |
|---------------------------|------------------------|-------------------------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses | `localhost:9092`        |
| `SCHEMA_REGISTRY_URL`     | Schema Registry URL    | `http://localhost:8081` |
| `KAFKA_TEST_TOPIC_PREFIX` | Prefix for test topics | `test-`                 |
