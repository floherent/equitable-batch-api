import os
import json
import pandas
from pathlib import Path
from typing import List, Dict, Optional

from .config import logger, Config


def prepare_scenarios(indir: str = Config.INPUT_DIR, override: bool = True, dirname: str = 'preprocessed'):
    filenames = [os.path.join(indir, f) for f in os.listdir(indir) if f.endswith('.csv')]
    if not filenames:
        logger.warning(f'<{indir}> does not contain any csv files.')
        return

    outdir = indir if override else os.path.join(indir, dirname)
    os.makedirs(outdir, exist_ok=True)
    logger.info(f'Preprocessed scenarios will be saved to {os.path.basename(outdir)}')

    columns = ['Scenario', 'TaxType', 'Gender', 'GuaranteeClass', 'CommissionType', 'IssueAge']
    for filename in filenames:
        dataframe = pandas.read_csv(filename, sep=',', header=0, index_col=False, usecols=columns)
        dataframe.to_csv(os.path.join(outdir, os.path.basename(filename)), index=False)
        logger.debug(f'{os.path.basename(filename)} has been preprocessed.')

    return outdir


class YearlyReport:
    """Represents a single yearly report with time-series data."""

    def __init__(self, data: List[List], columns: Optional[List[str]] = None):
        """
        Initialize a YearlyReport from parsed JSON data.

        Args:
            data: List of lists where first row is headers and subsequent rows are data
            columns: Optional list of column names (extracted from data if not provided)
        """
        if not data or len(data) < 1:
            raise ValueError("YearlyReport data must contain at least a header row")

        self.columns = columns or data[0]  # Extract headers from first row if not provided
        self.data = pandas.DataFrame(data[1:], columns=self.columns)  # Create DataFrame from the remaining rows

        # Convert numeric columns to appropriate types
        for col in self.columns:
            if col != 'Year':
                self.data[col] = pandas.to_numeric(self.data[col], errors='coerce')

        # Ensure Year is integer
        if 'Year' in self.columns:
            self.data['Year'] = pandas.to_numeric(self.data['Year'], errors='coerce').astype('Int64')

    def __repr__(self):
        return f'YearlyReport(rows={len(self.data)}, columns={self.columns})'

    def __str__(self):
        return f'<YearlyReport: {len(self.data)} rows, {len(self.columns)} columns>'

    @property
    def dataframe(self) -> pandas.DataFrame:
        """Get the underlying DataFrame."""
        return self.data

    def to_dict(self) -> Dict:
        """Convert to dictionary format."""
        return self.data.to_dict(orient='records')


class ScenarioResult:
    """Combines input scenario data with its corresponding output data."""

    def __init__(self, uuid: str, inputs: pandas.DataFrame, outputs: List[YearlyReport]):
        """
        Initialize a ScenarioResult.

        Args:
            uuid: The unique identifier for this scenario result
            inputs: DataFrame containing the flat input data
            outputs: List of YearlyReport objects, one per input row
        """
        self.uuid = uuid
        self.inputs = inputs
        self.outputs = outputs

        if len(inputs) != len(outputs):
            warning_msg = f"Mismatch between input rows ({len(inputs)}) and output rows ({len(outputs)}) for UUID {uuid}"
            logger.warning(warning_msg)

    def __repr__(self):
        return f'ScenarioResult(uuid={self.uuid}, inputs={len(self.inputs)}, outputs={len(self.outputs)})'

    def __str__(self):
        return f'<ScenarioResult::{self.uuid}: {len(self.inputs)} scenarios>'

    def __len__(self):
        return len(self.inputs)

    def __getitem__(self, idx: int) -> tuple:
        """Get a specific scenario input and its output."""
        if idx < 0 or idx >= len(self.inputs):
            raise IndexError(f"Index {idx} out of range for {len(self.inputs)} scenarios")

        input_row = self.inputs.iloc[idx]
        output_report = self.outputs[idx]
        return input_row, output_report

    @classmethod
    def from_files(cls, input_path: str, output_path: str) -> 'ScenarioResult':
        """
        Read and parse paired input/output files.

        Args:
            input_path: Path to the input CSV file
            output_path: Path to the output CSV file

        Returns:
            ScenarioResult object combining both files
        """

        uuid = Path(input_path).stem.replace('_input', '')  # Extract UUID from filename

        inputs_df = pandas.read_csv(input_path)  # Read input file (flat CSV structure)
        logger.debug(f"Read {len(inputs_df)} input rows from {Path(input_path).name}")

        outputs_df = pandas.read_csv(output_path)  # Read output file (JSON-formatted in YearlyReport column)
        yearly_reports = []  # Parse each JSON string in the YearlyReport column
        for idx, row in outputs_df.iterrows():
            try:
                json_data = json.loads(row['YearlyReport'])
                report = YearlyReport(json_data)
                yearly_reports.append(report)
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                logger.error(f"Error parsing row {idx} in {Path(output_path).name}: {e}")
                # Create an empty report as fallback
                yearly_reports.append(YearlyReport([['Year', 'DB Top-Ups', 'MB Top-Ups']]))

        logger.debug(f"Parsed {len(yearly_reports)} output reports from {Path(output_path).name}")

        return cls(uuid, inputs_df, yearly_reports)

    @classmethod
    def from_directory(cls, directory: str) -> List['ScenarioResult']:
        """
        Read all paired input/output files from a directory.

        Args:
            directory: Path to directory containing input/output CSV files

        Returns:
            List of ScenarioResult objects
        """
        dir_path = Path(directory)
        input_files = sorted(dir_path.glob('*_input.csv'))  # Find all input files
        results = []
        for input_file in input_files:
            uuid = input_file.stem.replace('_input', '')  # Construct corresponding output filename
            output_file = dir_path / f"{uuid}_output.csv"

            if not output_file.exists():
                logger.warning(f"Output file not found for {input_file.name}, skipping")
                continue

            try:
                result = cls.from_files(str(input_file), str(output_file))
                results.append(result)
            except Exception as e:
                logger.error(f"Error reading files for UUID {uuid}: {e}")
                continue

        logger.info(f"Loaded {len(results)} scenario results from {directory}")
        return results
