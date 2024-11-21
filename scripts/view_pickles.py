import pickle
import pandas as pd
from constants import STATE_DICT

def view_pickle(file_path):
    try:
        with open(file_path, 'rb') as file:
            data = pickle.load(file)
        
        if isinstance(data, pd.DataFrame):
            print(f"DataFrame shape: {data.shape}")
            print("First few rows:")
            print(data.head(10))
        else:
            print("Data type:", type(data))
            print("\nContent:")
            print(data)
    except Exception as e:
        print(f"Error loading pickle file: {e}")

def convert_pkl_to_csv(pkl_file, csv_file):
    try:
        df = pd.read_pickle(pkl_file)
        df.to_csv(csv_file, index=False)
        print(f"Successfully converted {pkl_file} to {csv_file}")
        
        print("\nFirst few rows of the CSV:")
        print(df.head(10))
        
    except Exception as e:
        print(f"Error converting pickle to CSV: {e}")

def print_first_rows(csv_file, num_rows=10):
    try:
        df = pd.read_csv(csv_file)
        print(f"\First {num_rows} rows of the CSV:")
        print(df.head(num_rows))    
    except Exception as e:
        print(f"Error printing first rows of CSV: {e}")

def get_num_files(folder_path):
    import os
    return len([name for name in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, name))])

view_pickle("zcta_data/population/TX/75025_population.pkl")
# view_pickle("output/household/NJ/08323_household.pkl")
# view_pickle("zcta_data/household/RI/02809_household.pkl")
# view_pickle("test/household/RI/02809_household.pkl")
# view_pickle("output/population/RI/02809_base_population.pkl")






