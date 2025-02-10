import numpy as np
import pandas as pd
import pickle
import os
import argparse
from typing import List, Dict, Optional
from agent_torch.data.census.census_loader import CensusDataLoader
from constants import POPULATION_DATA_PATH, HOUSEHOLD_DATA_PATH, STATE_DICT, AGE_GROUP_MAPPING
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('census_processing.log'),
        logging.StreamHandler()
    ]
)

def merge_ethnicities(path: str) -> None:
    """Merge ethnicities into major categories"""
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

def get_zctas(path: str) -> List[str]:
    """Get list of unique ZCTAs from a directory"""
    return list(set(
        filename.split('_')[0] 
        for filename in os.listdir(path) 
        if filename.endswith('.pkl')
    ))

def process_state(state: str, test=False) -> None:
    """Process all ZCTAs for a single state"""
    out_path = "test" if test else "output_v2"

    try:
        state_abbr = STATE_DICT[state][1]
        logging.info(f"Processing state: {state} ({state_abbr})")
        
        # Get list of ZCTAs for this state
        zcta_list = get_zctas(POPULATION_DATA_PATH + state_abbr)
        logging.info(f"Found {len(zcta_list)} ZCTAs for {state}")
        logging.info(POPULATION_DATA_PATH + state_abbr)
        
        # Process each ZCTA
        for zcta in zcta_list:
            try:
                logging.info(f"Processing ZCTA: {zcta}")
                
                # Create output directories
                Path(f"{out_path}/population/{state_abbr}").mkdir(parents=True, exist_ok=True)
                Path(f"{out_path}/household/{state_abbr}").mkdir(parents=True, exist_ok=True)
                
                # Merge ethnicities
                population_path = f'zcta_data/population/{state_abbr}/{zcta}_population.pkl'
                merge_ethnicities(population_path)
                
                # Load data
                household_data = pd.read_pickle(f"{HOUSEHOLD_DATA_PATH}/{state_abbr}/{zcta}_household.pkl")
                base_population_data = pd.read_pickle(f"{POPULATION_DATA_PATH}/{state_abbr}/{zcta}_population.pkl")
                
                # Initialize CensusDataLoader (without parallelization)
                census_data_loader = CensusDataLoader(n_cpu=1, use_parallel=False)
                
                # Generate base population
                census_data_loader.generate_basepop(
                    input_data=base_population_data,
                    region=state_abbr,
                    area_selector=None,
                    save_path=f"{out_path}/population/{state_abbr}/{zcta}_base_population.pkl",
                )
                
                # Generate household
                census_data_loader.generate_household(
                    household_data=household_data,
                    household_mapping=AGE_GROUP_MAPPING,
                    region=state_abbr,
                    save_path=f"{out_path}/household/{state_abbr}/{zcta}_household.pkl"
                )
                
                logging.info(f"Successfully processed ZCTA: {zcta}")
                
            except Exception as e:
                logging.error(f"Error processing ZCTA {zcta}: {str(e)}")
                continue
                
    except Exception as e:
        logging.error(f"Error processing state {state}: {str(e)}")

def get_state_batch(batch_number: int) -> List[str]:
    """Get the list of states for a given batch number"""
    all_states = list(STATE_DICT.keys())
    total_states = len(all_states)
    states_per_batch = (total_states + 9) // 10  # Ceiling division to distribute states evenly
    
    start_idx = (batch_number - 1) * states_per_batch
    end_idx = min(start_idx + states_per_batch, total_states)
    
    return all_states[start_idx:end_idx]

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Process census data for a batch of states or a single state')
    group = parser.add_mutually_exclusive_group(required=True)
    
    group.add_argument('--batch', type=int, help='Batch number (1-10)')
    group.add_argument('--state', type=str, help='State FIPS to process')
    args = parser.parse_args()
    
    if args.batch:
            # Validate batch number
            if args.batch < 1 or args.batch > 10:
                raise ValueError("Batch number must be between 1 and 10")
            
            # Get states for this batch
            states_to_process = get_state_batch(args.batch)
            
            logging.info(f"Processing batch {args.batch}")
            logging.info(f"States in this batch: {states_to_process}")
            
            # Process each state in the batch
            for state in states_to_process:
                process_state(state, test=False)
                logging.info(f"Completed processing state: {state}")

    elif args.state:
        state_FIPS = args.state

        # Process the state
        process_state(state_FIPS, test=False)
        logging.info(f"Completed processing state: {STATE_DICT[state_FIPS][0]}")

def debug():
    pass

if __name__ == "__main__":
    try:
        main()
        # debug()
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise