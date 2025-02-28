import pandas as pd
import datetime
import logging
import yaml
import pandas_market_calendars as mcal


# Set up the logger with a debug flag
def setup_logger(debug=False):
    """
    Set up the logger to print debug and info messages based on the debug flag.

    Parameters:
        debug (bool): Flag to turn on/off debug logging. Defaults to False (info level logging).

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(__name__)

    # Avoid adding multiple handlers if called multiple times
    if not logger.hasHandlers():
        if debug:
            logger.setLevel(logging.DEBUG)  # Show all logs if debug is True
        else:
            logger.setLevel(logging.INFO)   # Default to INFO level if debug is False

        console_handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


def load_config(config_file='config.yaml'):
    """
    Load the configuration parameters from the specified YAML file.

    Parameters:
        config_file (str): The path to the YAML configuration file. Defaults to 'config.yaml'.

    Returns:
        dict: The parsed configuration dictionary containing the early-close days and valid entry times.
    """
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
    return config['spx_day_time_config']


def generate_master_trading_day_list(start_date, end_date, debug=False):
    """
    Generate a list of all valid full trading days for SPX index options, excluding early-close days.
    Each day will contain the full set of valid entry times (26 entries: 0933, 0945, ..., 1545).

    Parameters:
        start_date (str): The starting date in 'YYYY-MM-DD' format.
        end_date (str): The ending date in 'YYYY-MM-DD' format.
        debug (bool): Flag to turn on/off debug logging. Defaults to False.

    Returns:
        pandas.DataFrame: A DataFrame containing all valid trading days and their respective entry times.

    Raises:
        ValueError: If start_date is later than end_date or if the dates are out of the valid range (2023-2025).
    """
    logger = setup_logger(debug)  # Initialize logger with the debug flag

    logger.debug("Starting to generate master trading day list.")

    # Load the config
    config = load_config()

    # Convert start and end dates to datetime objects
    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    # Raise an error if start_date is later than end_date
    if start_date > end_date:
        logger.error("start_date cannot be later than end_date.")
        raise ValueError("start_date cannot be later than end_date.")

    # Raise an error if dates are out of range (2023-2025)
    if start_date.year < 2023 or end_date.year > 2025:
        logger.error("Input dates must be between 2023 and 2025.")
        raise ValueError("Input dates must be between 2023 and 2025.")

    # Initialize the CBOE Index Options calendar for SPX index options
    cboe = mcal.get_calendar('CBOE_Index_Options')  # Correct calendar name for SPX index options
    logger.debug("CBOE Index Options calendar initialized.")

    # Get all valid trading days within the date range using the valid_days method
    valid_trading_days = cboe.valid_days(start_date, end_date)
    logger.info(f"Found {len(valid_trading_days)} valid trading days within the range.")

    # Early-close days for 2023 to 2025 (these are hardcoded based on market holidays)
    early_close_days = [datetime.datetime.strptime(day, "%Y-%m-%d").date() for day in config['early_close_days']]
    # Adding special holidays
    special_holidays = [datetime.datetime.strptime(day, "%Y-%m-%d").date() for day in config['special_holidays']]

    valid_times = config['valid_times']

    # Create a list to store all valid trading days and entry times
    trading_days = []

    # Loop through each valid trading day
    for day in valid_trading_days:
        # Exclude early-close days and special holidays
        if day.date() not in early_close_days and day.date() not in special_holidays:
            for time in valid_times:
                entry_datetime = datetime.datetime.combine(day, datetime.time.fromisoformat(time + ":00"))
                trading_days.append(entry_datetime)

    # Create DataFrame with the list of trading days and entry times
    trading_day_df = pd.DataFrame(trading_days, columns=["SpxDayTime"])  # Changed column name to 'SpxDayTime'

    # Debugging: Log the first 26 and last 26 rows of the master trading day list
    logger.debug("Generated master trading day list:")
    logger.debug(f"\nFirst 26 entries:\n{trading_day_df.head(26)}")
    logger.debug(f"\nLast 26 entries:\n{trading_day_df.tail(26)}")

    return trading_day_df