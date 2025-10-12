"""
Run only the analysis steps (Steps 3-5) assuming batch processing has already completed.

This script is useful when you already have the output files from batch processing
and just want to re-run the analysis (ranking and CTE calculations).

Usage:
    python run_analysis.py
"""

from dotenv import load_dotenv
from helpers import run_aggregate, run_cte0_calculation, run_cte80_calculation


def main():
    """
    Analysis-only pipeline:
    1. Aggregate and rank scenarios
    2. Calculate CTE0 (average across all scenarios)
    3. Calculate CTE80 (average across winner scenarios)
    """

    print('='*70)
    print('STARTING ANALYSIS PIPELINE (Steps 3-5)')
    print('='*70)

    # Step 1: Aggregate and rank scenarios
    print('\n[Step 1/3] Aggregating and ranking scenarios...')
    run_aggregate()
    print('[Step 1/3] Scenario ranking completed!')

    # Step 2: Calculate CTE0
    print('\n[Step 2/3] Calculating CTE0 (all scenarios)...')
    run_cte0_calculation()
    print('[Step 2/3] CTE0 calculation completed!')

    # Step 3: Calculate CTE80
    print('\n[Step 3/3] Calculating CTE80 (winner scenarios)...')
    run_cte80_calculation()
    print('[Step 3/3] CTE80 calculation completed!')

    # Summary
    print('\n' + '='*70)
    print('ANALYSIS PIPELINE COMPLETED SUCCESSFULLY!')
    print('='*70)
    print('Generated files:')
    print('  - final/scenarios_ranking.csv (scenario rankings with winners)')
    print('  - final/final_cte0.csv (CTE0 - average across all scenarios)')
    print('  - final/final_cte80.csv (CTE80 - average across winner scenarios)')
    print('='*70)


if __name__ == '__main__':
    load_dotenv()
    main()
