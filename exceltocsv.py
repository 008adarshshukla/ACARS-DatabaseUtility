import pandas as pd
import csv
import os

def convert_xlsx_to_csv(input_file, output_file):
     # Read the xlsx file
    df = pd.read_excel(input_file, header=None)
    
    # Extract column names from the first row
    column_names = df.iloc[0, 0].split(',')
    
    # Extract data rows and split values by comma
    data_rows = df.iloc[1:, 0].apply(lambda x: x.split(','))
    
    # Filter rows with the correct number of columns
    data_rows = data_rows[data_rows.apply(len) == len(column_names)]
    
    # Create a new DataFrame with the extracted values
    new_df = pd.DataFrame(data_rows.tolist(), columns=column_names)
    
    # Save to CSV
    new_df.to_csv(output_file, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"CSV file saved as: {output_file}")

# Define file paths on Mac Desktop
input_file = os.path.expanduser("~/Desktop/Approaches.xlsx")
output_file = os.path.expanduser("~/Desktop/Approaches_output.csv")

# Convert file
convert_xlsx_to_csv(input_file, output_file)
