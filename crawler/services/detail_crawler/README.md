# Detail Crawler Service

The Detail Crawler Service is responsible for crawling detailed property information from URLs that are provided via a Kafka topic. It processes the URLs, fetches the property details, and then sends the collected data to another Kafka topic.

## Features

-   Consumes URLs from a Kafka topic (`property-urls`)
-   Crawls property details with concurrent workers
-   Processes URLs in batches for efficiency
-   Implements retry mechanism for failed URLs
-   Sends crawled data to a Kafka topic (`property-data`)
-   Provides statistical reporting and monitoring
-   Auto-stop mechanism for use with batch processing tools like Airflow

## Configuration

The service can be configured using environment variables:

| Environment Variable | Default Value | Description                                              |
| -------------------- | ------------- | -------------------------------------------------------- |
| SOURCE               | batdongsan    | Source website (e.g., batdongsan, chotot)                |
| MAX_CONCURRENT       | 5             | Number of concurrent crawling workers                    |
| BATCH_SIZE           | 100           | Size of URL batches to process                           |
| RETRY_LIMIT          | 3             | Number of retries for failed URLs                        |
| RETRY_DELAY          | 5             | Delay in seconds between retries                         |
| CRAWLER_TYPE         | default       | Type of crawler to use                                   |
| OUTPUT_TOPIC         | property-data | Kafka topic to send results to                           |
| COMMIT_THRESHOLD     | 20            | Number of URLs to process before committing offsets      |
| COMMIT_INTERVAL      | 60            | Time in seconds between commits                          |
| RUN_ONCE_MODE        | false         | Whether to stop after a period of inactivity             |
| IDLE_TIMEOUT         | 3600          | Time in seconds to wait without messages before stopping |

## Usage

### Basic Usage

```bash
python main.py
```

### With Command-line Arguments

```bash
python main.py --source batdongsan --max-concurrent 10 --batch-size 200
```

### Auto-stop Mode (for use with Airflow or other schedulers)

```bash
python main.py --once --idle-timeout 300
```

In auto-stop mode, the service will automatically shut down after `idle_timeout` seconds without receiving any new messages. This is useful when running the service as part of a scheduled pipeline.

## Integration with Airflow

When using this service with Apache Airflow, you can use the `--once` flag to ensure the service stops after it's done processing all available data. This prevents having idle services running indefinitely after a DAG run is completed.

Example Airflow task:

```python
detail_crawler_task = BashOperator(
    task_id='run_detail_crawler',
    bash_command='cd /path/to/detail_crawler && python main.py --once --idle-timeout 300',
    dag=dag,
)
```

## Statistics and Monitoring

The service collects statistics on:

-   Number of URLs processed
-   Success and failure rates
-   Processing speed
-   Retry attempts
-   Kafka commit operations

These statistics are reported periodically and when the service stops.
