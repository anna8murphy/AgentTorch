import pickle
import pandas as pd

def view_pickle(file_path):
    try:
        with open(file_path, 'rb') as file:
            data = pickle.load(file)
        
        if isinstance(data, pd.DataFrame):
            print(f"DataFrame shape: {data.shape}")
            print("First few rows:")
            print(data.head(100))
            # print("\nColumn names:")
            # print(data.columns)
        else:
            print("Data type:", type(data))
            print("\nContent:")
            print(data)
    except Exception as e:
        print(f"Error loading pickle file: {e}")

# convert pkl to csv
def convert_pkl_to_csv(pkl_file, csv_file):
    try:
        # Read the pickle file
        df = pd.read_pickle(pkl_file)
        
        # Convert to CSV
        df.to_csv(csv_file, index=False)
        print(f"Successfully converted {pkl_file} to {csv_file}")
        
        # Print the first few rows of the CSV
        print("\nFirst few rows of the CSV:")
        print(df.head(50))
        
    except Exception as e:
        print(f"Error converting pickle to CSV: {e}")

# Example usage
# pkl_file = "NY_base_population.pkl"
# csv_file = "NY_base_population.csv"
# convert_pkl_to_csv(pkl_file, csv_file)

# print last 10 rows of csv
def print_first_rows(csv_file, num_rows=10):
    try:
        # Read the CSV file
        df = pd.read_csv(csv_file)
        
        # Print the last few rows
        print(f"\First {num_rows} rows of the CSV:")
        print(df.head(num_rows))    
    except Exception as e:
        print(f"Error printing first rows of CSV: {e}")

# view_pickle("pickles/NY_population_data.pkl")
# view_pickle("pickles/NY_household.pkl")

view_pickle("output/UT_base_population.pkl")

# csv_file = "NY_base_population.csv"
# print_first_rows(csv_file, num_rows=100)





