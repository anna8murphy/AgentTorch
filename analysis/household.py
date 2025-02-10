import pandas as pd
import numpy as np
import scipy.stats as stats
import pickle
import matplotlib.pyplot as plt
from constants import AGE_GROUP_MAPPING

def load_house_data(synthetic_path, real_path):
    """
    Load household data from pickle files.

    Args:
        synthetic_path (str): Path to the synthetic household data pickle file.
        real_path (str): Path to the real household data pickle file.

    Returns:
        tuple: A tuple containing the synthetic data and real data as pandas DataFrames.
    """
    with open(synthetic_path, 'rb') as file:
        synthetic_data = pickle.load(file)
    
    with open(real_path, 'rb') as file:
        real_data = pickle.load(file)
    
    return synthetic_data, real_data

def group_households(synthetic_data):
    """
    Group individuals into households based on synthetic data.

    Args:
        synthetic_data (pd.DataFrame): DataFrame containing synthetic household data.

    Returns:
        dict: A dictionary where the keys are household numbers and the values are lists of people.
    """
    res = {}
    for index, row in synthetic_data.iterrows():
        person = (row['age'], row['gender'], row['ethnicity'])
        house = row['household']
        if house not in res:
            res[house] = [person]
        else:
            res[house].append(person)
        
    return res

def synth_avg_household_size(households):
    """
    Calculate the average household size.

    Args:
        households (dict): A dictionary where the keys are household numbers and the values are lists of people.

    Returns:
        float: The average household size. If there are no households, returns 0.
    """
    total_people = sum(len(people) for people in households.values())
    total_households = len(households)
    if total_households == 0:
        return 0
    return total_people / total_households

def real_avg_household_size(file_path):
    with open(file_path, 'rb') as file:
        data = pickle.load(file)
        
    return data["average_household_size"].iloc[0]

def avg_adults_kids(households):
    """
    Calculate the average number of adults and kids per household.

    Args:
        households (dict): A dictionary where the keys are household numbers and the values are lists of people.

    Returns:
        tuple: The average number of adults and the average number of kids per household.
               If there are no households, returns (0, 0).
    """
    total_adults = sum(len([p for p in people if p[0] in AGE_GROUP_MAPPING['adult_list']]) for people in households.values())
    total_kids = sum(len([p for p in people if p[0] in AGE_GROUP_MAPPING['children_list']]) for people in households.values())
    total_households = len(households)
    if total_households == 0:
        return 0, 0
    return total_adults / total_households, total_kids / total_households

def plot_household_size(old, new, real):
    pass


def main():
    synthetic_path = "test/household/ME/04330_household.pkl"
    old_synthetic_path = "output/household/ME/04330_household.pkl"
    real_path = "zcta_data/household_v2/ME/04330_household.pkl"
    
    synthetic_data, real_data = load_house_data(synthetic_path, real_path)
    households = group_households(synthetic_data)

    old_synthetic_data, real_data = load_house_data(old_synthetic_path, real_path)
    old_households = group_households(old_synthetic_data)

    # for house in households:
    #     print(house, households[house])
    
    print("old average synth household size:", synth_avg_household_size(old_households))
    print("new average synth household size:", synth_avg_household_size(households))
    print("average real household size:", real_avg_household_size(real_path))

    print("old average adults, kids:", avg_adults_kids(old_households))
    print("new average adults, kids:", avg_adults_kids(households))

if __name__ == "__main__":
    main()