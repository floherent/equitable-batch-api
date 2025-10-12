from dotenv import load_dotenv
from helpers import (
    logger,
    prepare_scenarios,
    run_batch,
    run_aggregate,
    run_cte0_calculation,
    run_cte80_calculation
)


def main():
    """
    Complete pipeline:
    1. Prepare scenarios from input folder
    2. Run batch processing to generate outputs
    3. Aggregate and rank scenarios
    4. Calculate CTE0 (average across all scenarios)
    5. Calculate CTE80 (average across winner scenarios)
    """

    logger.debug('='*70)
    logger.info('STARTING COMPLETE PIPELINE')
    logger.debug('='*70)

    # Step 1: Prepare the scenarios
    logger.info('\n[Step 1/5] Preparing scenarios...')
    prepare_scenarios()
    logger.info('[Step 1/5] Scenarios prepared successfully!')

    # Step 2: Run batch processing
    logger.info('\n[Step 2/5] Running batch process...')
    run_batch()
    logger.info('[Step 2/5] Batch process completed!')

    # Step 3: Aggregate and rank scenarios
    logger.info('\n[Step 3/5] Aggregating and ranking scenarios...')
    run_aggregate()
    logger.info('[Step 3/5] Scenario ranking completed!')

    # Step 4: Calculate CTE0
    logger.info('\n[Step 4/5] Calculating CTE0 (all scenarios)...')
    run_cte0_calculation()
    logger.info('[Step 4/5] CTE0 calculation completed!')

    # Step 5: Calculate CTE80
    logger.info('\n[Step 5/5] Calculating CTE80 (winner scenarios)...')
    run_cte80_calculation()
    logger.info('[Step 5/5] CTE80 calculation completed!')

    # Summary
    logger.info('\n' + '='*70)
    logger.info('PIPELINE COMPLETED SUCCESSFULLY!')
    logger.info('='*70)
    logger.info('Generated files:')
    logger.info('  - outputs/*_input.csv and *_output.csv (batch results)')
    logger.info('  - final/scenarios_ranking.csv (scenario rankings with winners)')
    logger.info('  - final/final_cte0.csv (CTE0 - average across all scenarios)')
    logger.info('  - final/final_cte80.csv (CTE80 - average across winner scenarios)')
    logger.info('='*70)


if __name__ == '__main__':
    load_dotenv()
    main()
