import pandas as pd
import logging

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

def filter_early_close_data(df, master_trading_day_list, start_date, end_date, debug=False):
    """
    Filter out rows that correspond to entry times not in the master trading day list.
    The master trading day list contains valid entry times for full trading days.
    Rows outside the specified date range (start_date, end_date) will be filtered out.
    
    Parameters:
    df (pandas.DataFrame): The input DataFrame with trading data.
    master_trading_day_list (pandas.DataFrame): The master list of valid entry times for full trading days.
    start_date (str): The starting date in 'YYYY-MM-DD' format.
    end_date (str): The ending date in 'YYYY-MM-DD' format.
    debug (bool): Flag to turn on/off debug logging
    
    Returns:
    pandas.DataFrame: The filtered DataFrame with valid entry times.
    """
    logger = setup_logger(debug)  # Initialize logger with the debug flag
    
    logger.debug("Starting to filter early-close data.")
    
    # Convert start_date and end_date to full datetime values with time parts added
    start_datetime = pd.to_datetime(start_date + ' 00:00:00')  # Adding 00:00:00 as the time component for start
    end_datetime = pd.to_datetime(end_date + ' 23:59:59')  # Adding 23:59:59 as the time component for end

    logger.debug(f"Start date (with time): {start_datetime}")
    logger.debug(f"End date (with time): {end_datetime}")

    # Debugging: Print the first and last few rows of the original DataFrame (before any changes)
    logger.debug("First few rows from original DataFrame:")
    logger.debug(f"\n{df.head(26)}")
    logger.debug("Last few rows from original DataFrame:")
    logger.debug(f"\n{df.tail(26)}")
    
    # Cast EntryTime in df to datetime if it's not already in datetime format
    logger.debug("Casting EntryTime in df to datetime format.")
    df['EntryTime'] = pd.to_datetime(df['EntryTime'])
    
    # Debugging: Print the first and last few rows of the DataFrame after casting EntryTime
    logger.debug("First few rows after casting EntryTime:")
    logger.debug(f"\n{df.head(26)}")
    logger.debug("Last few rows after casting EntryTime:")
    logger.debug(f"\n{df.tail(26)}")
    
    # Filter rows based on the datetime range first, without changing EntryTime format
    logger.debug(f"Filtering rows between {start_datetime} and {end_datetime}.")
    df = df[(df['EntryTime'] >= start_datetime) & (df['EntryTime'] <= end_datetime)]

    # Debugging: Print the first and last few rows after filtering by date range
    logger.debug("First few rows after date range filter:")
    logger.debug(f"\n{df.head(26)}")
    logger.debug("Last few rows after date range filter:")
    logger.debug(f"\n{df.tail(26)}")
    
    logger.debug(f"Filtered DataFrame now has {len(df)} rows.")

    # Debugging: Check the format of EntryTime in the input dataset
    logger.debug("Sample EntryTime values from the input data (after casting):")
    logger.debug(f"\n{df['EntryTime'].head()}")

    # Cast SpxDayTime in master_trading_day_list to datetime format
    logger.debug("Casting SpxDayTime in master_trading_day_list to datetime format.")
    master_trading_day_list['SpxDayTime'] = pd.to_datetime(master_trading_day_list['SpxDayTime'])
    
    # Debugging: Print the first and last few rows of master_trading_day_list after casting SpxDayTime
    logger.debug("First few rows from master_trading_day_list after casting SpxDayTime:")
    logger.debug(f"\n{master_trading_day_list.head(26)}")
    logger.debug("Last few rows from master_trading_day_list after casting SpxDayTime:")
    logger.debug(f"\n{master_trading_day_list.tail(26)}")

    # Filter rows in the input dataframe where the EntryTime is in the valid entry times list
    logger.debug("Filtering rows based on SpxDayTime from master trading day list.")
    filtered_df = df[df['EntryTime'].isin(master_trading_day_list['SpxDayTime'])]
    
    # Debugging: Print the first and last few rows after filtering by SpxDayTime
    logger.debug("First few rows after filtering by SpxDayTime:")
    logger.debug(f"\n{filtered_df.head(26)}")
    logger.debug("Last few rows after filtering by SpxDayTime:")
    logger.debug(f"\n{filtered_df.tail(26)}")
    
    logger.debug(f"Filtered DataFrame has {len(filtered_df)} rows after entry time validation.")
    
    return filtered_df
