import pandas as pd
from SpxDayTime import generate_master_trading_day_list  # Import the function from SpxDayTime.py
from filter_early_close_data import filter_early_close_data  # Import the function from filter_early_close_data.py

def test_filter_early_close_data():
    # Step 1: Define the date range
    start_date = '2023-12-23'
    end_date = '2023-12-31'
    
    # Step 2: Load the test input CSV (input_trading_data.csv)
    df = pd.read_csv('input_trading_data.csv')

    # Step 3: Apply the filter_early_close_data function (it will generate the master list internally)
    filtered_df = filter_early_close_data(df, start_date, end_date)  # Pass start_date and end_date

    # Step 4: Print the filtered DataFrame for debugging purposes
    print(filtered_df)
    
    # Step 5: Check the EntryTime and SpxDayTime formats
    print("EntryTime from input data:")
    print(df['EntryTime'])
    
    print("\nSpxDayTime from master trading day list:")
    print(filtered_df['EntryTime'])
    
    # Step 6: Test expected rows
    expected_rows = [
        '2023-01-03 09:33:00',  # Valid entry time
        '2023-01-03 10:15:00',  # Valid entry time
    ]
    
    # Step 7: Assertion for expected rows
    assert all(row in filtered_df['EntryTime'].astype(str).values for row in expected_rows), "Test failed!"

    # Step 8: Save filtered DataFrame to a CSV file (optional)
    filtered_df.to_csv('filtered_trading_data.csv', index=False)

# Run the test
test_filter_early_close_data()
