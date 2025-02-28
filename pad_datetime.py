import pandas as pd
import logging

def setup_logger(debug=False):
    """
    Set up the logger to print debug and info messages based on the debug flag.
    
    Parameters:
    debug (bool): Flag to turn on/off debug logging. Defaults to False (info level logging).
    
    Returns:
    logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger('pad_entry_times_logger')  # Use a consistent logger name
    
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

def pad_entry_times(df, master_trading_day_list, start_date, end_date, debug=False):
    logger = setup_logger(debug)  # Set up logger for debugging

    logger.debug("Starting to pad entry times.")

    # Convert the start and end dates to datetime objects for comparison
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    # Extract all valid entry times (from the master list) and filter by the date range
    valid_entry_times_set = set(pd.to_datetime(master_trading_day_list['SpxDayTime']))
    
    # Filter only by date portion, ignoring time
    valid_entry_times_set = {time for time in valid_entry_times_set if start_date.date() <= time.date() <= end_date.date()}

    # Debugging: Print the first and last few rows of the input DataFrame 'df' before any processing
    logger.debug(f"First few rows of the input DataFrame (before processing):\n{df.head(26)}")
    logger.debug(f"Last few rows of the input DataFrame (before processing):\n{df.tail(26)}")

    # Extract the existing entry times from the dataframe and filter by the date range
    df['EntryTime'] = pd.to_datetime(df['EntryTime'])  # Ensure EntryTime is a datetime object
    existing_entry_times_set = set(df['EntryTime'])

    # Debugging: Print the first and last few entries of existing_entry_times_set
    logger.debug(f"First few entries from existing entry times(b4 filter):\n{sorted(list(existing_entry_times_set))[:26]}")
    logger.debug(f"Last few entries from existing entry times(b4 filter):\n{sorted(list(existing_entry_times_set))[-26:]}")
    
    # Filter only by date portion, ignoring time
    existing_entry_times_set = {time for time in existing_entry_times_set if start_date.date() <= time.date() <= end_date.date()}

    # Identify missing entry times by comparing with the master list
    missing_entry_times_set = valid_entry_times_set - existing_entry_times_set

    # Debugging: Print the first and last few entries of valid_entry_times_set
    logger.debug(f"First few entries from valid entry times:\n{sorted(list(valid_entry_times_set))[:26]}")
    logger.debug(f"Last few entries from valid entry times:\n{sorted(list(valid_entry_times_set))[-26:]}")

    # Debugging: Print the first and last few entries of existing_entry_times_set
    logger.debug(f"First few entries from existing entry times:\n{sorted(list(existing_entry_times_set))[:26]}")
    logger.debug(f"Last few entries from existing entry times:\n{sorted(list(existing_entry_times_set))[-26:]}")

    # Debugging: Print the first and last few entries of missing_entry_times_set
    logger.debug(f"First few missing entry times:\n{sorted(list(missing_entry_times_set))[:26]}")
    logger.debug(f"Last few missing entry times:\n{sorted(list(missing_entry_times_set))[-26:]}")

    padded_rows = []
    for missing_time in missing_entry_times_set:
        padded_rows.append({
            'EntryTime': missing_time,
            'TradeID': 0,
            'OptionType': "",
            'Delta': "",
            'ShortStrike': "",
            'LongStrike': "",
            'Width': "",
            'Premium': "",
            'ProfitTarget': "",
            'ProfitDateTime': "",
            'ProfitPrice': "",
            'StopLossDateTime': "",
            'StopLossTarget': "",
            'StopLossPrice': "",
            'IsWin': "",
            'Outcome': "",
            'ProfitLoss': 0,
            'ProfitLossAfterSlippage': 0,
            'CommissionFees': "",
            'Slippage': "",
            'LossMultiple': "",
            'TradesToday': "",
            'VIX': "",
            'OpenDate': "",
            'OpenTime': "",
            'CloseDate': "",
            'CloseTime': ""
        })

    padded_df = pd.DataFrame(padded_rows)

    # Append the padded rows to the original DataFrame
    df = pd.concat([df, padded_df], ignore_index=True)

    # Sort the DataFrame by EntryTime to maintain the correct order
    df = df.sort_values(by="EntryTime").reset_index(drop=True)

    logger.debug(f"Padded DataFrame (after sorting and appending):")
    logger.debug(f"\n{df.head()}")

    return df