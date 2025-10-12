## Asynchronous Batch Processing

This example serves as a starting point for developers to create batch jobs using
the [SDK][sdk]. It provides a basic structure and configuration setup to quickly
get started with batch processing tasks.

## Setup

Create or edit your `.env` file in the project root folder and add your Spark environment
URL and API authentication details:

```bash
CSPARK_BASE_URL="https://spark.my-env.coherent.global/my-tenant"
CSPARK_API_KEY="my-api-key"
```

These environment variables are used by the SDK to authenticate and connect to
your Spark environment.

## Configuration

Batch inputs and options are specified in [`config.py`](helpers/config.py). Modify
this file to adjust your batch processing settings, including:

- Input CSV file location
- Chunk size
- Number of chunks
- Service URI

## Usage

### Complete Pipeline (Recommended)

Run the complete end-to-end pipeline that includes batch processing and analysis:

```bash
poetry run python main.py
```

This will execute the following steps:

1. **Prepare Scenarios** - Process input CSV files from the `inputs/` folder
2. **Batch Processing** - Send data to Spark service and generate outputs
3. **Scenario Ranking** - Aggregate DB and MB Top-Ups, rank scenarios, identify top 20% winners
4. **CTE0 Calculation** - Calculate average Top-Ups across all scenarios for each year
5. **CTE80 Calculation** - Calculate average Top-Ups across winner scenarios only for each year

**Generated Output Files:**

- `outputs/*_input.csv` and `*_output.csv` - Batch processing results
- `final/scenarios_ranking.csv` - Scenario rankings with winner designation
- `final/final_cte0.csv` - CTE0 (average across all scenarios)
- `final/final_cte80.csv` - CTE80 (average across winner scenarios)

### Analysis Only

If you already have batch processing outputs and just want to re-run the analysis:

```bash
poetry run python run_analysis.py
```

This will skip the batch processing (Steps 1-2) and only run:

- Scenario ranking
- CTE0 calculation
- CTE80 calculation

## Pipeline Details

### Step 1-2: Batch Processing

The script reads input CSV files, splits them into chunks, and processes each
chunk asynchronously. The batch processing status is displayed in the console.

### Step 3: Scenario Ranking

- Aggregates all DB and MB Top-Ups for each scenario across all permutations
- Ranks scenarios from most negative (best) to most positive (worst)
- Identifies the top 20% as "winners"
- Outputs: `final/scenarios_ranking.csv`

### Step 4: CTE0 (Conditional Tail Expectation - All Scenarios)

- Calculates the average of DB and MB Top-Ups for each year
- Uses data from ALL scenarios
- Outputs: `final/final_cte0.csv`

### Step 5: CTE80 (Conditional Tail Expectation - Winners Only)

- Calculates the average of DB and MB Top-Ups for each year
- Uses data from WINNER scenarios only (top 20%)
- Outputs: `final/final_cte80.csv`

## Customization

The pipeline can be customized by modifying:

- [`config.py`](helpers/config.py) - Batch settings and directory paths
- [`main.py`](main.py) - Pipeline orchestration
- [`aggregate.py`](helpers/aggregate.py) - Analysis logic and calculations

<!-- References -->
[sdk]: https://github.com/Coherent-Partners/spark-python-sdk
