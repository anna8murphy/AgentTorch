import pickle
import pandas as pd

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

# view_pickle("zcta_data/population/AK/99929_population.pkl")
view_pickle("zcta_data/population/NJ/08323_population.pkl")
# view_pickle("output/population/NJ/08323_base_population.pkl")
# convert_pkl_to_csv("output/population/NJ/08323_base_population.pkl", "scripts/08323_base_population.csv")
# convert_pkl_to_csv("zcta_data/population/NJ/08323_population.pkl", "scripts/08323_population.csv")




