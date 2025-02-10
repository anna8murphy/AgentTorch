import pickle
import pandas as pd
from constants import STATE_DICT
import os
from multiprocessing import Pool

def view_pickle(file_path):
    try:
        with open(file_path, 'rb') as file:
            data = pickle.load(file)
        
        if isinstance(data, pd.DataFrame):
            print(f"DataFrame shape: {data.shape}")
            print("First few rows:")
            print(data.head(10))
            # print(data["average_household_size"])
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
        pd.set_option('display.max_columns', None)
        df = pd.read_csv(csv_file)
        print(f"\First {num_rows} rows of the CSV:")
        print(df.head(num_rows)) 
    except Exception as e:
        print(f"Error printing first rows of CSV: {e}")

def get_num_files(folder_path):
    import os
    return len([name for name in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, name))])

import os
import pandas as pd
from multiprocessing import Pool

def rename_area(file_path):
    """
    Rename the area based on the file path.

    Args:
        file_path (str): Path to the file.

    Returns:
        str: The new area name.
    """
    parts = file_path.split('/')
    state = parts[-2].strip()
    zcta = (parts[-1].split('_'))[0].strip()
    new_area = state + zcta
    return new_area

def convert_file_to_parquet(file_path):
    """
    Convert a pickle file to a parquet file and rename the 'area' column.

    Args:
        file_path (str): Path to the pickle file.
    """
    new_area = rename_area(file_path)

    # Load the pickle file
    df = pd.read_pickle(file_path)

    # Rename the 'area' column
    df['area'] = new_area

    # Save the DataFrame as a parquet file
    output_file_path = file_path.replace(".pkl", ".parquet")
    df.to_parquet(output_file_path)

def convert_to_parquet():
    """
    Convert all pickle files in the specified directories to parquet files and rename the 'area' column.
    """
    population_dir = "output_v2/population"
    state_folders_pop = [f for f in os.listdir(population_dir) if os.path.isdir(os.path.join(population_dir, f))]

    household_dir = "output_v2/household"
    state_folders_house = [f for f in os.listdir(household_dir) if os.path.isdir(os.path.join(household_dir, f))]

    parquet_dir = "output_v2/parquet"
    parquet_population_dir = os.path.join(parquet_dir, "population")
    parquet_household_dir = os.path.join(parquet_dir, "household")
    os.makedirs(parquet_population_dir, exist_ok=True)
    os.makedirs(parquet_household_dir, exist_ok=True)

    for state_folder_pop in state_folders_pop:
        state_folder_path_pop = os.path.join(population_dir, state_folder_pop)
        parquet_state_folder_path_pop = os.path.join(parquet_population_dir, state_folder_pop)
        os.makedirs(parquet_state_folder_path_pop, exist_ok=True)
        file_paths_pop = [os.path.join(state_folder_path_pop, file_name) for file_name in os.listdir(state_folder_path_pop) if file_name.endswith(".pkl")]
        with Pool() as pool:
            pool.map(convert_file_to_parquet, file_paths_pop)
        print(f"Completed processing state")

    for state_folder_house in state_folders_house:
        state_folder_path_house = os.path.join(household_dir, state_folder_house)
        parquet_state_folder_path_house = os.path.join(parquet_household_dir, state_folder_house)
        os.makedirs(parquet_state_folder_path_house, exist_ok=True)
        file_paths_house = [os.path.join(state_folder_path_house, file_name) for file_name in os.listdir(state_folder_path_house) if file_name.endswith(".pkl")]
        with Pool() as pool:
            pool.map(convert_file_to_parquet, file_paths_house)

convert_to_parquet()

# view_pickle("zcta_data/population/TX/75025_population.pkl")
# view_pickle("output/household/NJ/08323_household.pkl")
# view_pickle("zcta_data/household_v2/ME/19977_household.pkl")
# view_pickle("output_v2/household/NY/06390_household.pkl")
# view_pickle("output/population/RI/02809_base_population.pkl")

# # get file count of all state folders in output_v2/population
# import os

# # Get all state folders in output_v2/population
# population_dir = "output_v2/population"
# state_folders = [f for f in os.listdir(population_dir) if os.path.isdir(os.path.join(population_dir, f))]

# # Count files in each state folder
# total_files = 0
# for state in state_folders:
#     state_path = os.path.join(population_dir, state)
#     num_files = len([f for f in os.listdir(state_path) if os.path.isfile(os.path.join(state_path, f))])
#     total_files += num_files
#     print(f"{state}: {num_files} files")

# print(f"\nTotal files across all states: {total_files}")




