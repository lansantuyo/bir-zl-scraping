import os
import re
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from comparison_utils import convert_to_numeric, is_extra_whitespace, is_case_difference, is_numeric_rounding, is_missing_value, is_fill_forward_issue, is_header_included, is_cutoff_difference
import datetime



class RemarkGenerator:
    """
    Generates human-readable remarks about differences between two dataframe values.

    Attributes:
        issue_patterns (dict): Dictionary mapping issue descriptions to validation functions.
    """
    def __init__(self):
        """Initialize the RemarkGenerator with predefined issue patterns."""
        self.issue_patterns = {}

        # Register known issues with their respective checking functions
        self.add_issue("Extra whitespace difference", is_extra_whitespace)
        self.add_issue("Case difference", is_case_difference)
        self.add_issue("Numeric rounding difference", is_numeric_rounding)
        self.add_issue("Missing value", is_missing_value)
        self.add_issue("Incorrect fill forward", is_fill_forward_issue)
        self.add_issue("Table headers included in output", is_header_included)

    def add_issue(self, description, check_function):
        """
        Adds a new type of issue to be checked.

        Args:
            description (str): The human-readable description of the issue.
            check_function (function): The function that validates the issue.
        """
        self.issue_patterns[description] = check_function

    def generate_remark(self, df1_value, df2_value):
        """
        Generates a remark describing the type of difference between two values.

        Args:
            df1_value: The value from the first dataframe.
            df2_value: The value from the second dataframe.

        Returns:
            str: A descriptive remark about the difference.
        """
        for remark, check in self.issue_patterns.items():
            if check(df1_value, df2_value):
                return remark
        return "Unclassified difference"
        

class DirectoryDataFrameComparator:
    """
    Compares data files (Excel) from two directories and identifies differences.

    Attributes:
        input_dir_1 (str): Path to the first directory.
        input_dir_2 (str): Path to the second directory.
        output_dir (str or None): Optional path to output comparison results.
        directory_files (dict): Stores the files found in each directory.
        remark_generator (RemarkGenerator): An instance of RemarkGenerator for categorizing differences.
    """
    
    def __init__(self, input_dir_1: str, input_dir_2: str, remark_generator=RemarkGenerator(), 
                 output_dir=None):
        """
        Initializes the comparator and validates input directories.

        Args:
            input_dir_1 (str): Path to the first directory.
            input_dir_2 (str): Path to the second directory.
            remark_generator (RemarkGenerator, optional): A remark generator instance.
            output_dir (str, optional): Path to save output files.
        """
        try:
            self.input_dir_1 = self._validate_path(input_dir_1)
            self.input_dir_2 = self._validate_path(input_dir_2)
            self.output_dir = self._validate_path(output_dir, is_output_dir=True) if output_dir else output_dir
            self.directory_files = {
                "input_dir_1": self._read_directory(self.input_dir_1),
                "input_dir_2": self._read_directory(self.input_dir_2)
            }
            self.remark_generator = remark_generator
        except Exception as e:
            print(f"Error initializing comparator: {e}")

    def _validate_path(self, directory:str, is_output_dir=False) -> str:
        """
        Validates a directory path.

        Args:
            directory (str): The directory path to validate.
            is_output_dir (bool): Whether the directory is meant for output.

        Returns:
            str: The validated directory path.

        Raises:
            FileNotFoundError: If the directory does not exist.
            NotADirectoryError: If the path is not a directory.
            PermissionError: If access is not permitted.
        """
        if not os.path.exists(directory):
            raise FileNotFoundError(f"Directory '{directory}' does not exist.")
            
        if not os.path.isdir(directory):
            raise NotADirectoryError(f"'{directory}' is not a directory.")

        if not os.access(directory, os.R_OK):
            raise DirectoryPermissionError(f"Cannot read files from directory '{directory}'.")

        if is_output_dir:
            if not os.access(directory, os.W_OK):
                raise DirectoryPermissionError(f"Cannot write to directory '{directory}'.")

        return directory

    def _read_directory(self, directory: str):
        """
        Reads all files from a directory.

        Args:
            directory (str): Path to the directory.

        Returns:
            list: List of file paths.
        """
        dir = Path(directory)
        dir_files = []
        try:
            for item in dir.iterdir():
                dir_files.append(item)
            return dir_files
        except Exception as e:
            print(f"Error reading directory: {e}")

    def _match_directory_files(self, directory_files: dict):
        """
        Matches files from two directories based on a common pattern.

        Args:
            directory_files (dict): Dictionary containing lists of files from both directories.

        Returns:
            list: List of matched file pairs.
        """
        if not directory_files['input_dir_1'] or not directory_files['input_dir_2']:
            raise ValueError("One or both directories are empty. No comparisons can be made.")
        try:
            rdo_pattern = re.compile(r"RDO No\. \d+[A-Z]?")
            dir1_files = {}
            dir2_files = {}

            for filepath in directory_files.get('input_dir_1', []):
                match = rdo_pattern.search(filepath.name)
                if match:
                    rdo_key = match.group()
                    dir1_files[rdo_key] = filepath

            for filepath in directory_files.get('input_dir_2', []):
                match = rdo_pattern.search(filepath.name)
                if match:
                    rdo_key = match.group()
                    dir2_files[rdo_key] = filepath

            matched_filepairs = [(dir1_files[rdo_key], dir2_files[rdo_key]) 
                                 for rdo_key in dir1_files.keys() & dir2_files.keys()]

            return matched_filepairs
        except Exception as e:
            raise e

    def _compare_excel_files(self, filepair: tuple):
        """
        Compares two Excel files cell-by-cell.

        Args:
            filepair (tuple): A pair of file paths.

        Returns:
            pd.DataFrame: A DataFrame containing the differences.
        """
        try:
            df1 = pd.read_excel(str(filepair[0]))
            df2 = pd.read_excel(str(filepair[1]))

            # Align DataFrames to ensure same shape (fills missing values with NaN)
            df1, df2 = df1.align(df2, join="outer", axis=1)  # Align columns
            df1, df2 = df1.align(df2, join="outer", axis=0)  # Align rows
            
            # if not df1.columns.equals(df2.columns):
            #     raise ValueError("DataFrames have different column structures.")
            df1 = df1.fillna('')
            df2 = df2.fillna('')
            
            mask = df1.ne(df2)
            differences = []
            
            for col in df1.columns:
                diff_rows = mask[col]
                if diff_rows.any():
                    for index in df1.index[diff_rows]:
                        df1_value = df1.at[index, col]
                        df2_value = df2.at[index, col]
                        remark = self.remark_generator.generate_remark(df1_value, df2_value)
                        differences.append({
                            'idx': index,
                            'df1_value': df1_value,
                            'df2_value': df2_value,
                            'column': col,
                            'remarks': remark,
                            'df1_filename': filepair[0].name,
                            'df2_filename': filepair[1].name
                        })

            diff_df = pd.DataFrame(differences)
            return diff_df
            
        except Exception as e:
            print(f"Error comparing files {filepair[0]} and {filepair[1]}: {e}")
            return None

    def run(self, to_file=False, verbose_logs=False, unique_only=False):
        """
        Runs the file comparison process.

        Args:
            to_file (bool): Whether to save results to a file.
            verbose_logs (bool): Whether to print detailed logs.
            unique_only (bool): Whether to display only the first instance of an identified difference.

        Returns:
            pd.DataFrame: The cumulative differences found.
        """
        cumulative_diff_df = pd.DataFrame(columns=['idx', 'df1_value', 'df2_value', 'column',
                                                  'remarks', 'df1_filename', 'df2_filename'])
        matched_filepairs = self._match_directory_files(self.directory_files)
        for filepair in tqdm(matched_filepairs, desc="Comparing files", unit="pair"):
            if verbose_logs:
                print(f"Comparing: {filepair[0].name} with {filepair[1].name}")
            diff_df = self._compare_excel_files(filepair)
            
            if diff_df is not None:  # Avoid concatenating None
                cumulative_diff_df = pd.concat([cumulative_diff_df, diff_df], ignore_index=True)
        
        print("Comparisons complete!")

        if unique_only:
            mask = cumulative_diff_df.drop('idx', axis=1).columns
            cumulative_diff_df = cumulative_diff_df.drop_duplicates(subset=mask)
            
        if to_file:
            print("Writing to csv...")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"cumulative_diff_{timestamp}.csv"
            filepath = os.path.join(self.output_dir if self.output_dir else ".", filename)
            cumulative_diff_df.to_csv(filepath, encoding='utf-8')
            print(f"Successfully written file to {filepath}")
        return cumulative_diff_df
        