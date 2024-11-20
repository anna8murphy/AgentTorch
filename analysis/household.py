import pandas as pd
import numpy as np
import scipy.stats as stats
import pickle
import matplotlib.pyplot as plt
from constants import AGE_GROUP_MAPPING

def load_house_data(synthetic_path, real_path):
    with open(synthetic_path, 'rb') as file:
        synthetic_data = pickle.load(file)
    
    with open(real_path, 'rb') as file:
        real_data = pickle.load(file)
    
    return synthetic_data, real_data

def group_households(synthetic_data):
    res = {}
    for index, row in synthetic_data.iterrows():
        person = (row['age'], row['gender'], row['ethnicity'])
        house = row['household']
        if (house not in res):
            res[house] = [person]
        else:
            res[house].append(person)
        
    return res

def avg_household_size(households):
    # households is dict of {household num: list of people}
    total_people = sum(len(people) for people in households.values())
    total_households = len(households)
    if total_households == 0:
        return 0
    return total_people, total_people / total_households

def avg_adults_kids(households):
    # households is dict of {household num: list of people}
    total_adults = sum(len([p for p in people if p[0] in AGE_GROUP_MAPPING['adult_list']]) for people in households.values())
    total_kids = sum(len([p for p in people if p[0] in AGE_GROUP_MAPPING['children_list']]) for people in households.values())
    total_households = len(households)
    if total_households == 0:
        return 0, 0
    return total_adults / total_households, total_kids / total_households

def main():
    synthetic_path = "output/household/NJ/08323_household.pkl"
    real_path = "zcta_data/household/NJ/08323_household.pkl"
    
    synthetic_data, real_data = load_house_data(synthetic_path, real_path)
    households = group_households(synthetic_data)
    
    # print("average household size:", avg_household_size(households))
    # print("average adults per household, average kids per household:", avg_adults_kids(households))
    
if __name__ == "__main__":
    main()