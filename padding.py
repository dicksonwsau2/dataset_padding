import os
import shutil
import pandas as pd
import logging
from multiprocessing import Pool
from SpxDayTime import generate_master_trading_day_list  # Ensure this is the correct import
from filter_early_close_data import filter_early_close_data  # Assuming already implemented
from tqdm import tqdm  # Import tqdm for progress bar
import multiprocessing
from pad_datetime import pad_entry_times  # Import the pad_entry_times function
import argparse  # Import argparse to handle command-line arguments

def setup_logger(debug=False):
    """
    Set up the logger to print debug and info messages based on the debug flag.

    Parameters:
        debug (bool): Flag to turn on/off debug logging. Defaults to False (info level logging).

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger('main_logger')  # Use a consistent logger name

    # Check if the logger already has handlers to avoid adding multiple handlers in different parts of the code
    if not logger.hasHandlers():
        if debug:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

        console_handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger

def worker(args):
    """
    Worker function to handle filtering for each worker's dataset.
    Filters data based on valid entry times from the master trading day list.

    Parameters:
        args (tuple): A tuple containing the arguments (input_file, master_trading_day_list, start_date, end_date, output_directory).
    """
    input_file, master_trading_day_list, start_date, end_date, output_directory = args

    logger = setup_logger(debug=False)  # Set up logger for each worker

    logger.debug(f"Worker started for {input_file}")

    try:
        df = pd.read_csv(input_file)
        logger.debug(f"Loaded data from {input_file} with {len(df)} rows")

        # Step 1: Filter early-close data with date range
        df = filter_early_close_data(df, master_trading_day_list, start_date, end_date, debug=False)
        logger.debug(f"Filtered data for {input_file}. Remaining rows: {len(df)}")

        # Step 2: Pad missing entry times by comparing with the master trading day list
        df = pad_entry_times(df, master_trading_day_list, start_date, end_date, debug=False)
        logger.debug(f"Padded data for {input_file}. Total rows after padding: {len(df)}")

        # Step 3: Define the output file path
        output_file = os.path.join(output_directory, f'{os.path.basename(input_file)}')

        # Step 4: Save the final filtered and padded DataFrame for each worker's dataset
        df.to_csv(output_file, index=False)
        logger.debug(f"Saved processed data to {output_file}")

    except Exception as e:
        logger.error(f"Error processing {input_file}: {e}")

    logger.debug(f"Worker completed for {input_file}")

def parse_arguments():
    """
    Parse command-line arguments for the script.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Filter and pad SPX option data.\n\n"
            "This script processes SPX option data by applying the following stages:\n"
            "1. **Loading and Reading Data**: The script loads CSV files containing SPX option trading data from a specified directory.\n"
            "2. **Filtering Early-Close Data**: It filters out rows corresponding to early-close days (e.g., holidays) based on a master list of valid trading days and the specified date range.\n"
            "3. **Padding Missing Entry Times**: It ensures all required entry times (from 09:33 to 15:45 with intervals) are present for each trading day. If any entry times are missing, the script will pad the dataset with placeholders.\n"
            "4. **Saving Processed Data**: After filtering and padding, the processed data is saved into a new CSV file in the specified output directory.\n"
            "The script processes files in parallel using multiple CPU cores for faster execution."
        )
    )

    # Define command-line arguments
    parser.add_argument('--src_dir', type=str, required=True, help="Path to the directory containing CSV files. All CSV files in this directory will be processed.")
    parser.add_argument('--output_dir', type=str, required=True, help="Path to the output directory where the processed files will be saved.")
    parser.add_argument('--s_date', type=str, required=True, help="Start date in 'YYYY-MM-DD' format. This specifies the beginning of the data range to be processed.")
    parser.add_argument('--e_date', type=str, required=True, help="End date in 'YYYY-MM-DD' format. This specifies the end of the data range to be processed.")

    # Return the parsed arguments
    return parser.parse_args()

def main(debug=False):
    """
    Main function to generate the master trading day list and process worker data.

    Parameters:
        debug (bool): Flag to enable debug-level logging.
    """
    # Parse command-line arguments
    args = parse_arguments()

    logger = setup_logger(debug)  # Set up logger for the main function

    logger.debug("Starting main function")

    try:
        # Generate and save the master trading day list
        master_trading_day_list = generate_master_trading_day_list(args.s_date, args.e_date)
        logger.info(f"Generated master trading day list with {len(master_trading_day_list)} entries")

        # Save to a CSV file
        master_trading_day_list.to_csv("master_trading_day_list.csv", index=False)
        logger.info("Master trading day list saved to master_trading_day_list.csv")

        # Check if the output directory exists, if so, remove it and raise an error if removal fails
        if os.path.exists(args.output_dir):
            try:
                shutil.rmtree(args.output_dir)
                logger.info(f"Deleted existing directory: {args.output_dir}")
            except Exception as e:
                logger.error(f"Failed to delete existing directory {args.output_dir}: {e}")
                raise

        # Create the output directory if it doesn't exist, raise an error if creation fails
        try:
            os.makedirs(args.output_dir)
            logger.info(f"Created output directory: {args.output_dir}")
        except Exception as e:
            logger.error(f"Failed to create output directory {args.output_dir}: {e}")
            raise

        # Get a list of input files from the source directory
        input_files = [f for f in os.listdir(args.src_dir) if f.endswith('.csv')]  # Example worker files

        # Adding a progress bar for the pool of worker tasks
        logger.info("Starting worker tasks...")

        # Using multiprocessing Pool to run workers and adding a progress bar
        with Pool(processes=int(multiprocessing.cpu_count() - 1)) as pool:
            with tqdm(total=len(input_files), desc="Processing files") as pbar:
                for _ in pool.imap_unordered(
                        worker,
                        [(os.path.join(args.src_dir, input_file), master_trading_day_list, args.s_date, args.e_date, args.output_dir)
                         for input_file in input_files]
                ):
                    pbar.update(1)

        logger.info("Main function completed")

    except Exception as e:
        logger.error(f"Error in main function: {e}")
        raise  # Re-raise the error to stop the execution if necessary

if __name__ == "__main__":
    main(debug=False)