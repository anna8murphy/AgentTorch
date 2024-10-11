import numpy as np
import pandas as pd
from agent_torch.data.census.census_loader import CensusDataLoader
from constants import POPULATION_DATA_PATH, HOUSEHOLD_DATA_PATH, STATE_DICT, AGE_GROUP_MAPPING

for state in STATE_DICT:    
    state_abbr = STATE_DICT[state][1]
    household_data = pd.read_pickle(HOUSEHOLD_DATA_PATH + state_abbr + "_household.pkl")
    base_population_data = pd.read_pickle(POPULATION_DATA_PATH + state_abbr + "_population_data.pkl")

    area_selector = None
    geo_mapping = None

    census_data_loader = CensusDataLoader(n_cpu=8, use_parallel=True)

    # census_data_loader.generate_basepop(
    #     input_data=base_population_data,  # The population data frame
    #     region=state_abbr,  # The target region for generating base population
    #     area_selector=area_selector,  # Area selection criteria, if applicable
    #     save_path=f"output/{state_abbr}_base_population.pkl",
    # )

    census_data_loader.generate_household(
        household_data=household_data,  # The loaded household data
        household_mapping=AGE_GROUP_MAPPING,  # Mapping of age groups for household composition
        region=state_abbr,  # The target region for generating households
        save_path=f"output/{state_abbr}_household.pkl"
    )

