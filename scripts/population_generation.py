import numpy as np
import pandas as pd
import pickle
import os
from agent_torch.data.census.census_loader import CensusDataLoader
from constants import POPULATION_DATA_PATH, HOUSEHOLD_DATA_PATH, STATE_DICT, AGE_GROUP_MAPPING

def get_zctas(path):
    zcta_list = []
    
    for filename in os.listdir(path):
        if filename.endswith('.csv'):
            zcta = filename.split('_')[0]
            
            if zcta not in zcta_list:
                zcta_list.append(zcta)
        
    # includes state abbr
    return zcta_list

# bin ethnicities into "other" category
def merge_ethnicities(path):
    with open(path, 'rb') as file:
        population_data = pickle.load(file)
    
    ethnicity_df = population_data['ethnicity']
    keep = ['White', 'Asian', 'Black']

    def categorize(row):
        if row['ethnicity'] in keep:
            return row['ethnicity']
        else:
            return 'Other'
        
    ethnicity_df['category'] = ethnicity_df.apply(categorize, axis=1)
    merged_df = ethnicity_df.groupby(['area', 'region', 'category'])['count'].sum().reset_index()
    merged_df = merged_df.rename(columns={'category': 'ethnicity'})

    population_data['ethnicity'] = merged_df

    with open(path, 'wb') as file:
        pickle.dump(population_data, file)

state = "36"
state_abbr = "NY"
state_zctas = get_zctas("data/age_gender")

for zcta in state_zctas:
    #### RUN ONCE:
    # try:
    #     filename = 'data/population/' + state_abbr + zcta + '_population_data.pkl'
    #     merge_ethnicities(filename)
    # except:
    #     continue

    state_abbr = STATE_DICT[state][1]
    household_data = pd.read_pickle(HOUSEHOLD_DATA_PATH  + zcta + "_household.pkl")
    base_population_data = pd.read_pickle(POPULATION_DATA_PATH + zcta + "_population_data.pkl")

    area_selector = None
    geo_mapping = None

    census_data_loader = CensusDataLoader(n_cpu=8, use_parallel=True)

    census_data_loader.generate_basepop(
        input_data=base_population_data,
        region=state_abbr,
        area_selector=area_selector,
        save_path=f"output/population/{zcta}_base_population.pkl",
    )

    census_data_loader.generate_household(
        household_data=household_data,
        household_mapping=AGE_GROUP_MAPPING,
        region=state_abbr,
        save_path=f"output/household/{zcta}_household.pkl"
    )