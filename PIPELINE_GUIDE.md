# Pipeline Guide

## Overview

This project provides a complete end-to-end pipeline for batch processing scenario
data and performing comprehensive analysis including ranking and CTE calculations.

## Pipeline Architecture

```text
┌─────────────────────────────────────────────────────────────────┐
│                    COMPLETE PIPELINE                            │
└─────────────────────────────────────────────────────────────────┘

inputs/                        outputs/
  └── *.csv                      ├── UUID_input.csv (M scenarios * P permutations => N files)
       │                         └── UUID_output.csv (M scenarios * P permutations => N files)
       │                                  │
       ▼                                  ▼
┌──────────────┐            ┌──────────────────────┐
│   Step 1-2   │            │      Step 3-5        │
│ Batch Process│───────────▶│  Analysis Pipeline   │
└──────────────┘            └──────────────────────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    ▼                  ▼                  ▼
final/scenarios_ranking.csv   final/final_cte0.csv   final/final_cte80.csv
```

## Components

### Core Files

1. **`main.py`** - Complete 5-step pipeline
   - Prepare scenarios
   - Run batch processing
   - Aggregate and rank scenarios
   - Calculate CTE0
   - Calculate CTE80

2. **`run_analysis.py`** - Analysis-only pipeline (Steps 3-5)
   - Skips batch processing
   - Use when you already have outputs

3. **`helpers/`** - Core modules
   - `models.py` - Data models (ScenarioResult, YearlyReport)
   - `aggregate.py` - Analysis functions
   - `batch.py` - Batch processing logic
   - `config.py` - Configuration settings

## Usage

### Option 1: Complete Pipeline (All Steps)

```bash
poetry run python main.py
```

**What it does:**

1. Reads from `inputs/` folder
2. Processes scenarios through Spark service
3. Generates UUID-based input/output files in `outputs/`
4. Ranks scenarios and identifies winners
5. Calculates CTE0 (all scenarios)
6. Calculates CTE80 (winner scenarios only)

**Time:** ~2-5 minutes (depending on batch size)

### Option 2: Analysis Only (Skip Batch Processing)

```bash
poetry run python run_analysis.py
```

**What it does:**

1. Reads existing files from `outputs/`
2. Ranks scenarios and identifies winners
3. Calculates CTE0 (all scenarios)
4. Calculates CTE80 (winner scenarios only)

**Time:** ~15-30 seconds

## Output Files

### 1. final/scenarios_ranking.csv

Ranks all scenarios from best (most negative) to worst (most positive).

```csv
Scenario,DB+MB Total,Rank,Winner
1,-6464.35403255242,1,True
2,-149.87455594234584,2,False
```

**Columns:**

- `Scenario` - Scenario number
- `DB+MB Total` - Sum of all DB and MB Top-Ups across all permutations
- `Rank` - Position (1 = best)
- `Winner` - True if in top 20%

### 2. final/final_cte0.csv

Average DB and MB Top-Ups across **ALL** scenarios for each year.

```csv
Year,DB Top-Ups,MB Top-Ups
2025,-0.01240261881215265,0.0
2026,-0.3629931770434537,0.0
...
2125,-2.419006742031203e-28,0.0
```

**Usage:** Overall average expectations across all possible scenarios.

### 3. final/final_cte80.csv

Average DB and MB Top-Ups across **WINNER** scenarios only for each year.

```csv
Year,DB Top-Ups,MB Top-Ups
2025,-0.014354882884435937,0.0
2026,-0.4201309919484417,0.0
...
2125,-2.799776e-28,0.0
```

**Usage:** Conservative tail risk estimate using only best-performing scenarios.

## Key Concepts

### Scenario Ranking

- Each scenario has multiple permutations (up to 864)
- Each permutation generates 101 years of data
- Total = Sum of all DB + MB Top-Ups (excluding years) across all permutations
- Lower (more negative) totals are "better"
- Top 20% are marked as "winners"

### CTE0 (Conditional Tail Expectation - All)

- Average of DB and MB Top-Ups for each year
- Uses **ALL** scenarios and permutations
- Provides overall expected values

### CTE80 (Conditional Tail Expectation - 80th Percentile)

- Average of DB and MB Top-Ups for each year
- Uses **ONLY WINNER** scenarios (top 20%)
- Provides conservative tail risk estimate
- Generally more negative than CTE0

## Customization

### Changing Winner Threshold

In `helpers/aggregate.py`, modify line ~92:

```python
# Change 0.2 to desired percentage (e.g., 0.1 for top 10%)
top_20_percent_count = max(1, int(len(ranking_df) * 0.2))
```

### Changing Output Directories

In `helpers/config.py`, modify:

```python
class Config:
    INPUT_DIR = 'inputs'        # Input CSV directory
    OUTPUT_DIR = 'outputs'      # Output CSV directory
```

### Adding Custom Metrics

In `helpers/aggregate.py`, add new calculation functions following the pattern:

```python
def calculate_custom_metric(results: list[ScenarioResult]) -> pd.DataFrame:
    # Your logic here
    pass

def run_custom_calculation(output_dir: str = Config.OUTPUT_DIR):
    results = ScenarioResult.from_directory(output_dir)
    metric_df = calculate_custom_metric(results)
    metric_df.to_csv('custom_metric.csv', index=False)
```

Then add to `main.py` or `run_analysis.py`.

## Troubleshooting

### "No scenario results found"

- Ensure `outputs/` folder has `*_input.csv` and `*_output.csv` files
- Check that files follow UUID naming convention

### "No winner scenarios found"

- Check `final/scenarios_ranking.csv` exists
- Verify it has a `Winner` column with at least one `True` value

### Import errors

- Ensure all files are in correct directories
- Run: `poetry install` to install dependencies

## Dependencies

- Python 3.8+
- pandas
- cspark.sdk
- python-dotenv

## Architecture Notes

- **ScenarioResult** class handles loading and pairing input/output files
- **YearlyReport** class parses JSON-formatted output data
- All aggregation functions are in `helpers/aggregate.py`
- Configuration centralized in `helpers/config.py`

## Performance

- Batch processing: Variable (depends on Spark service and data size)
- Analysis pipeline: ~15-30 seconds for 50 UUID pairs (1000 scenarios)
- Memory usage: ~100-500MB depending on dataset size

## Next Steps

1. Run the complete pipeline: `poetry run python main.py`
2. Review the output files
3. Customize thresholds or add new metrics as needed
4. Use `run_analysis.py` for quick re-analysis

---

For more information, see `readme.md` or check the inline documentation in each module.
