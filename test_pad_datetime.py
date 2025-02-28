import unittest
import pandas as pd
from pad_datetime import pad_entry_times

class TestPadEntryTimes(unittest.TestCase):

    def test_padding(self):
        # Create a sample DataFrame with all columns and arbitrary values
        df_data = {
            'TradeID': [5015667, 5016231, 5027967],
            'EntryTime': pd.to_datetime(['2023-01-03 09:33:00', '2023-01-03 10:00:00', '2023-01-03 10:15:00']),
            'OptionType': ['P', 'P', 'C'],
            'Delta': [0.0, 0.0, 0.0],
            'ShortStrike': [3825.0, 3840.0, 3860.0],
            'LongStrike': [3770.0, 3785.0, 3915.0],
            'Width': [55.0, 55.0, 55.0],
            'Premium': [4.7, 4.1, 4.45],
            'ProfitTarget': ['P100', 'P100', 'P100'],
            'ProfitDateTime': ["", "", ""],  # Use empty string for missing values
            'ProfitPrice': ["", "", ""],  # Use empty string for missing values
            'StopLossDateTime': ['1/3/2023 10:18:00 AM', '1/3/2023 9:52:00 AM', ""],  # Use empty string for missing value
            'StopLossTarget': ['2x', '2x', '2x'],
            'StopLossPrice': [14.1, 12.3, ""],  # Use empty string for missing value
            'IsWin': [False, False, True],
            'Outcome': ['Stop Loss', 'Stop Loss', 'Expiration'],
            'ProfitLoss': [-9.4, -8.2, 4.45],
            'ProfitLossAfterSlippage': [-9.4, -8.2, 4.45],
            'CommissionFees': [0, 0, 0],
            'Slippage': [2.0, 2.0, 0],
            'LossMultiple': [26, 0, 26],
            'TradesToday': [23.09, 23.09, 23.09],
            'VIX': [23.09, 23.09, 23.09],
            'OpenDate': ['2023-01-03', '2023-01-03', '2023-01-03'],
            'OpenTime': ['09:33:00', '09:45:00', '10:00:00'],
            'CloseDate': ['2023-01-03', '2023-01-03', '2023-01-03'],
            'CloseTime': ['10:18:00', '09:52:00', '10:15:00']
        }
        
        df = pd.DataFrame(df_data)
        
        # Save the sample DataFrame to a CSV file
        df.to_csv('test_pad_datetime_data.csv', index=False)

        # Create a sample master trading day list
        master_data = {
            'SpxDayTime': pd.to_datetime([
                '2023-01-03 09:33:00', '2023-01-03 09:45:00', '2023-01-03 10:00:00', 
                '2023-01-03 10:15:00', '2023-01-03 10:30:00', '2023-01-03 10:45:00'])
        }
        master_trading_day_list = pd.DataFrame(master_data)
        master_trading_day_list.to_csv('test_pad_datetime_masterlist.csv', index=False)

        # Define start and end date for padding
        start_date = '2023-01-01'
        end_date = '2023-12-31'

        # Read the input DataFrame from CSV file
        df = pd.read_csv('test_pad_datetime_data.csv')
        
        # Debugging: print the DataFrame after reading from CSV
        print("DataFrame after reading from CSV:")
        print(df)

        df['EntryTime'] = pd.to_datetime(df['EntryTime'])  # Ensure correct datetime format

        # Read the master trading day list from CSV file
        master_trading_day_list = pd.read_csv('test_pad_datetime_masterlist.csv')
        master_trading_day_list['SpxDayTime'] = pd.to_datetime(master_trading_day_list['SpxDayTime'])  # Ensure correct datetime format
        
        # Pad the datetime entries
        padded_df = pad_entry_times(df, master_trading_day_list, start_date, end_date, debug=False)

        # Debugging: Print the padded dataframe
        print("Padded DataFrame:")
        print(padded_df)

        # Save the padded DataFrame to a CSV file
        padded_df.to_csv('test_pad_datetime_output.csv', index=False)

        # Assert that the new datetime entries have been added
        for time in pd.to_datetime(['2023-01-03 09:33:00', '2023-01-03 09:45:00', '2023-01-03 10:00:00']):
            self.assertTrue(time in padded_df['EntryTime'].values, f"Missing {time} in padded data.")
        
        # Check the order and the length
        self.assertEqual(len(padded_df), len(master_trading_day_list), "Length mismatch after padding.")
        self.assertListEqual(list(padded_df['EntryTime'].sort_values().dt.strftime('%Y-%m-%d %H:%M:%S').values),
                             list(master_trading_day_list['SpxDayTime'].sort_values().dt.strftime('%Y-%m-%d %H:%M:%S').values),
                             "Order mismatch after padding.")

        # Assert no extra dates outside the defined range (start_date, end_date)
        self.assertTrue(
            padded_df['EntryTime'].min() >= pd.to_datetime(start_date),
            f"EntryTime before start date {start_date} found."
        )
        self.assertTrue(
            padded_df['EntryTime'].max() <= pd.to_datetime(end_date),
            f"EntryTime after end date {end_date} found."
        )

        # Assert that the arbitrary values in columns like 'OptionType' are still intact after padding
        self.assertEqual(padded_df.loc[padded_df['EntryTime'] == '2023-01-03 09:33:00', 'OptionType'].values[0], 'P')
        self.assertEqual(padded_df.loc[padded_df['EntryTime'] == '2023-01-03 10:00:00', 'ShortStrike'].values[0], 3840.0)

if __name__ == '__main__':
    unittest.main()
