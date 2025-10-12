import sys
import os
import csv
import pandas as pd
from collections import defaultdict

from .models import ScenarioResult
from .config import logger, Config

csv.field_size_limit(sys.maxsize)


def aggregate_scenario_results(results: list[ScenarioResult]) -> pd.DataFrame:
    """
    Aggregate DB and MB Top-Ups for all scenarios across all permutations.

    Args:
        results: List of ScenarioResult objects

    Returns:
        DataFrame with columns: Scenario, DB+MB Total
    """
    # Dictionary to store aggregated totals by scenario number
    scenario_totals = defaultdict(float)

    logger.info(f"Starting aggregation for {len(results)} result files...")

    # Process each result file
    for result in results:
        logger.debug(f"Processing {result.uuid} with {len(result)} scenarios")

        # Iterate through each input-output pair in the result
        for idx in range(len(result)):
            input_row, output_report = result[idx]
            scenario_num = input_row['Scenario']  # Get the scenario number
            output_df = output_report.dataframe  # Get the output DataFrame

            # Sum all DB Top-Ups and MB Top-Ups for this permutation
            db_total = output_df['DB Top-Ups'].sum()
            mb_total = output_df['MB Top-Ups'].sum()

            # Add to the scenario total
            combined_total = db_total + mb_total
            scenario_totals[scenario_num] += combined_total

            logger.debug(
                f"  Scenario {scenario_num}: DB={db_total:.6f}, MB={mb_total:.6f}, "
                f"Combined={combined_total:.6f} (Running total: {scenario_totals[scenario_num]:.6f})"
            )

    logger.info(f"Aggregation complete. Found {len(scenario_totals)} unique scenarios")

    # Convert to DataFrame and sort by scenario number
    ranking_df = pd.DataFrame([
        {'Scenario': scenario, 'DB+MB Total': total}
        for scenario, total in scenario_totals.items()
    ])

    ranking_df = ranking_df.sort_values('Scenario').reset_index(drop=True)
    return ranking_df


def calculate_cte0(results: list[ScenarioResult]) -> pd.DataFrame:
    """
    Calculate CTE0 - the average of DB and MB Top-Ups across all scenarios for each year.

    Args:
        results: List of ScenarioResult objects

    Returns:
        DataFrame with columns: Year, DB Top-Ups, MB Top-Ups (averaged across all scenarios)
    """
    # Dictionary to store yearly data: {year: {'db': [values], 'mb': [values]}}
    yearly_data = defaultdict(lambda: {'db': [], 'mb': []})

    logger.info(f"Calculating CTE0 for {len(results)} result files...")

    # Collect all values by year across all scenarios
    for result in results:
        logger.debug(f"Processing {result.uuid} with {len(result)} scenarios")

        # Iterate through each input-output pair in the result
        for idx in range(len(result)):
            input_row, output_report = result[idx]

            # Get the output DataFrame
            output_df = output_report.dataframe

            # Add each year's values to the collection
            for _, row in output_df.iterrows():
                year = row['Year']
                db_value = row['DB Top-Ups']
                mb_value = row['MB Top-Ups']

                yearly_data[year]['db'].append(db_value)
                yearly_data[year]['mb'].append(mb_value)

    logger.info(f"Collected data for {len(yearly_data)} years")

    # Calculate averages for each year
    cte0_data = []
    for year in sorted(yearly_data.keys()):
        db_values = yearly_data[year]['db']
        mb_values = yearly_data[year]['mb']

        avg_db = sum(db_values) / len(db_values) if db_values else 0
        avg_mb = sum(mb_values) / len(mb_values) if mb_values else 0

        cte0_data.append({
            'Year': int(year),
            'DB Top-Ups': avg_db,
            'MB Top-Ups': avg_mb
        })

        logger.debug(f"Year {year}: DB avg = {avg_db:.6f} (from {len(db_values)} values), "
                     f"MB avg = {avg_mb:.6f} (from {len(mb_values)} values)")

    cte0_df = pd.DataFrame(cte0_data)
    logger.info(f"CTE0 calculation complete. {len(cte0_df)} years processed")

    return cte0_df


def calculate_cte80(results: list[ScenarioResult], winner_scenarios: list[int]) -> pd.DataFrame:
    """
    Calculate CTE80 - the average of DB and MB Top-Ups across only winner scenarios for each year.

    Args:
        results: List of ScenarioResult objects
        winner_scenarios: List of scenario numbers that are winners

    Returns:
        DataFrame with columns: Year, DB Top-Ups, MB Top-Ups (averaged across winner scenarios only)
    """
    # Dictionary to store yearly data: {year: {'db': [values], 'mb': [values]}}
    yearly_data = defaultdict(lambda: {'db': [], 'mb': []})

    logger.info(f"Calculating CTE80 for {len(results)} result files using winner scenarios: {winner_scenarios}...")

    # Collect all values by year across winner scenarios only
    for result in results:
        logger.debug(f"Processing {result.uuid} with {len(result)} scenarios")

        # Iterate through each input-output pair in the result
        for idx in range(len(result)):
            input_row, output_report = result[idx]
            scenario_num = input_row['Scenario']  # Get the scenario number

            # Skip if this scenario is not a winner
            if scenario_num not in winner_scenarios:
                continue
            output_df = output_report.dataframe  # Get the output DataFrame

            # Add each year's values to the collection
            for _, row in output_df.iterrows():
                year = row['Year']
                db_value = row['DB Top-Ups']
                mb_value = row['MB Top-Ups']

                yearly_data[year]['db'].append(db_value)
                yearly_data[year]['mb'].append(mb_value)

    logger.info(f"Collected data for {len(yearly_data)} years")

    # Calculate averages for each year
    cte80_data = []
    for year in sorted(yearly_data.keys()):
        db_values = yearly_data[year]['db']
        mb_values = yearly_data[year]['mb']

        avg_db = sum(db_values) / len(db_values) if db_values else 0
        avg_mb = sum(mb_values) / len(mb_values) if mb_values else 0

        cte80_data.append({
            'Year': int(year),
            'DB Top-Ups': avg_db,
            'MB Top-Ups': avg_mb
        })

        logger.debug(f"Year {year}: DB avg = {avg_db:.6f} (from {len(db_values)} values), "
                     f"MB avg = {avg_mb:.6f} (from {len(mb_values)} values)")

    cte80_df = pd.DataFrame(cte80_data)
    logger.info(f"CTE80 calculation complete. {len(cte80_df)} years processed")

    return cte80_df


def run_cte0_calculation(output_dir: str = Config.OUTPUT_DIR, export_dir: str = 'final'):
    """
    Calculate CTE0 and export to a CSV file.

    Args:
        output_dir: Directory containing input/output CSV files
        export_dir: Directory to save the CTE0 CSV (default: final)
    """

    logger.info("="*60)
    logger.info("Starting CTE0 calculation...")
    logger.info("="*60)

    # Load all scenario results
    logger.info(f"Loading scenario results from {output_dir}...")
    results = ScenarioResult.from_directory(output_dir)

    if not results:
        logger.error("No scenario results found. Exiting.")
        return

    logger.info(f"Loaded {len(results)} result files")
    cte0_df = calculate_cte0(results)  # Calculate CTE0

    filename = "final_cte0.csv"
    filepath = os.path.join(export_dir, filename)

    # Display summary
    logger.info("="*60)
    logger.info("CTE0 SUMMARY")
    logger.info("="*60)
    logger.info(f"Total years: {len(cte0_df)}")
    logger.info(f"Year range: {cte0_df['Year'].min()} - {cte0_df['Year'].max()}")
    logger.info(f"\nFirst 10 rows:")
    logger.info(f"\n{cte0_df.head(10)}")
    logger.info(f"\nLast 10 rows:")
    logger.info(f"\n{cte0_df.tail(10)}")

    # Export to CSV
    logger.info(f"\n{'='*60}")
    logger.info(f"Exporting CTE0 to {filepath}...")
    cte0_df.to_csv(filepath, index=False)
    logger.info(f"Successfully exported {len(cte0_df)} years to {filepath}")

    logger.info("="*60)
    logger.info("CTE0 calculation complete!")
    logger.info("="*60)

    return cte0_df, filepath


def run_cte80_calculation(
    output_dir: str = Config.OUTPUT_DIR,
    ranking_file: str = "final/scenarios_ranking.csv",
    export_dir: str = 'final'
):
    """
    Calculate CTE80 (using only winner scenarios) and export to a CSV file.

    Args:
        output_dir: Directory containing input/output CSV files
        ranking_file: Path to scenarios ranking CSV file (default: final/scenarios_ranking.csv)
        export_dir: Directory to save the CTE80 CSV (default: final)
    """

    logger.info("="*60)
    logger.info("Starting CTE80 calculation...")
    logger.info("="*60)

    # Load ranking file to get winner scenarios
    logger.info(f"Loading ranking file from {ranking_file}...")
    ranking_df = pd.read_csv(ranking_file)

    # Get winner scenarios
    winners = ranking_df[ranking_df['Winner'] == True]
    winner_scenarios = winners['Scenario'].tolist()

    logger.info(f"Found {len(winner_scenarios)} winner scenario(s): {winner_scenarios}")

    if not winner_scenarios:
        logger.error("No winner scenarios found. Exiting.")
        return

    # Load all scenario results
    logger.info(f"Loading scenario results from {output_dir}...")
    results = ScenarioResult.from_directory(output_dir)

    if not results:
        logger.error("No scenario results found. Exiting.")
        return

    logger.info(f"Loaded {len(results)} result files")

    cte80_df = calculate_cte80(results, winner_scenarios)

    filename = "final_cte80.csv"
    filepath = os.path.join(export_dir, filename)

    # Display summary
    logger.info("="*60)
    logger.info("CTE80 SUMMARY")
    logger.info("="*60)
    logger.info(f"Winner scenarios used: {winner_scenarios}")
    logger.info(f"Total years: {len(cte80_df)}")
    logger.info(f"Year range: {cte80_df['Year'].min()} - {cte80_df['Year'].max()}")
    logger.info(f"\nFirst 10 rows:")
    logger.info(f"\n{cte80_df.head(10)}")
    logger.info(f"\nLast 10 rows:")
    logger.info(f"\n{cte80_df.tail(10)}")

    # Export to CSV
    logger.info(f"\n{'='*60}")
    logger.info(f"Exporting CTE80 to {filepath}...")
    cte80_df.to_csv(filepath, index=False)
    logger.info(f"Successfully exported {len(cte80_df)} years to {filepath}")

    logger.info("="*60)
    logger.info("CTE80 calculation complete!")
    logger.info("="*60)

    return cte80_df, filepath


def run_aggregate(output_dir: str = Config.OUTPUT_DIR, export_path: str = None):
    """
    Runs the aggregation process to create a ranking of scenarios.

    Args:
        output_dir: Directory containing input/output CSV files
        export_path: Path to save the ranking CSV (default: final/scenarios_ranking.csv)
    """
    if export_path is None:
        export_path = "final/scenarios_ranking.csv"

    # Create final directory if it doesn't exist
    os.makedirs(os.path.dirname(export_path), exist_ok=True)

    logger.info("="*60)
    logger.info("Starting scenario ranking aggregation...")
    logger.info("="*60)

    # Load all scenario results
    logger.info(f"Loading scenario results from {output_dir}...")
    results = ScenarioResult.from_directory(output_dir)

    if not results:
        logger.error("No scenario results found. Exiting.")
        return

    logger.info(f"Loaded {len(results)} result files")

    # Aggregate the results
    ranking_df = aggregate_scenario_results(results)

    # Sort by DB+MB Total (most negative to most positive)
    ranking_df = ranking_df.sort_values('DB+MB Total', ascending=True).reset_index(drop=True)

    # Add Rank column (1 = most negative, which is "best" in this context)
    ranking_df['Rank'] = range(1, len(ranking_df) + 1)

    # Calculate top 20% threshold
    top_20_percent_count = max(1, int(len(ranking_df) * 0.2))

    # Mark winners (top 20% = most negative values)
    ranking_df['Winner'] = ranking_df['Rank'] <= top_20_percent_count

    # Get winner scenarios
    winners_df = ranking_df[ranking_df['Winner'] == True].copy()
    winner_scenarios = winners_df['Scenario'].tolist()

    # Display summary
    logger.info("="*60)
    logger.info("AGGREGATION SUMMARY")
    logger.info("="*60)
    logger.info(f"Total scenarios: {len(ranking_df)}")
    logger.info(f"Ranking order: Most negative (best) to most positive (worst)")
    logger.info(f"\nTop 20% threshold: {top_20_percent_count} scenario(s)")
    logger.info(f"Winner scenarios: {winner_scenarios}")

    logger.info(f"\n{'='*60}")
    logger.info("COMPLETE RANKING (Most Negative to Most Positive)")
    logger.info(f"{'='*60}")
    logger.info(f"\n{ranking_df.to_string(index=False)}")

    logger.info(f"\n{'='*60}")
    logger.info("TOP 20% WINNERS")
    logger.info(f"{'='*60}")
    logger.info(f"\n{winners_df.to_string(index=False)}")

    # Export to CSV
    logger.info(f"\n{'='*60}")
    logger.info(f"Exporting ranking to {export_path}...")
    ranking_df.to_csv(export_path, index=False)
    logger.info(f"Successfully exported {len(ranking_df)} scenarios to {export_path}")

    logger.info("="*60)
    logger.info("Aggregation complete!")
    logger.info("="*60)

    return ranking_df
