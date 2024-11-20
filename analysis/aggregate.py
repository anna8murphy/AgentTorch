from household import load_house_data, group_households, avg_household_size, avg_adults_kids
from demographic import load_data, compare_median_age, chi_square_test, mann_whitney_u_test, get_total_pop
from constants import STATE_DICT

import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import random
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

def get_zctas(url):
    response = requests.get(url)
    response.raise_for_status() 

    soup = BeautifulSoup(response.content, 'html.parser')
    zctas = []

    table = soup.find('table', {'class': 'table table-striped table-bordered'}) 
    if table:
        rows = table.find_all('tr')
        for row in rows[1:]:  
            cols = row.find_all('td')
            if cols:
                zcta = cols[0].text.strip() 
                state = cols[1].text.strip()
                state = state.split(" ")[-1]
                zctas.append([zcta, state])

    return zctas

def get_zcta_data(zcta_lst, density):
    """
    Load and analyze ZCTA data, including demographic and household statistics.
    
    Parameters:
    zcta_lst (list of tuples): List of (zcta, state) tuples
    density (float): Population density for the ZCTAs
    
    Returns:
    pandas.DataFrame: DataFrame containing ZCTA data and analysis
    """
    data = []
    
    for zcta, state in zcta_lst:
        try:
            demographic_s_path = f"output/population/{state}/{zcta}_base_population.pkl"
            household_s_path = f"output/household/{state}/{zcta}_household.pkl"

            demographic_r_path = f"zcta_data/population/{state}/{zcta}_population.pkl"
            household_r_path = f"zcta_data/household/{state}/{zcta}_household.pkl"

            synthetic_data, real_data = load_data(demographic_s_path, demographic_r_path)

            # Demographic analysis
            med_age = compare_median_age(synthetic_data, real_data)
            chi2_synthetic, p_synthetic, chi2_real, p_real = chi_square_test(synthetic_data, real_data)
            u_statistic_m, p_value_m, u_statistic_f, p_value_f = mann_whitney_u_test(synthetic_data, real_data)
            synthetic_total_pop, real_total_pop = get_total_pop(synthetic_data, real_data)

            # Household analysis
            synthetic_data, real_data = load_house_data(household_s_path, household_r_path)
            households = group_households(synthetic_data)
            total_people, avg_size = avg_household_size(households)
            real_avg_size = real_data['average_household_size'].values[0]
            avg_adults, avg_kids = avg_adults_kids(households)

            data.append({
                "zcta": zcta,
                "state": state,
                "synth_avg_size": avg_size,
                "real_avg_size": real_avg_size,
                "synth_avg_adults": avg_adults,
                "synth_avg_kids": avg_kids,
                "synth_total_pop": synthetic_total_pop,
                "synth_total_pop_from_house": total_people,
                "real_total_pop": real_total_pop
            })
        except Exception as e:
            print(f"Error processing {zcta}, {state}: {e}")

    return pd.DataFrame(data)

def get_random_zctas(base_path="zcta_data/population"):
    '''
    Get 2 random ZCTAs from each state. Returns list of [zcta, state_abbr].
    '''
    res = []
    for state_code, (state_name, state_abbr) in STATE_DICT.items():
        state_folder = os.path.join(base_path, state_abbr)
        if not os.path.exists(state_folder):
            print(f"Folder for {state_name} ({state_abbr}) not found.")
            continue
        
        files = [[f, state_abbr] for f in os.listdir(state_folder) if f.endswith("_population.pkl")]
        files_stripped = [[f.split("_")[0], state_abbr] for f, _ in files]
        
        random_files = random.sample(files_stripped, 2)
        res.extend(random_files)

    return res

def plot_household_size(data):
   states, synth_sizes, real_sizes = zip(*data)
   
   states = np.array(states)
   synth_sizes = np.array(synth_sizes)
   real_sizes = np.array(real_sizes)
   
   outlier_mask = synth_sizes > 10
   outlier_states = states[outlier_mask]
   outlier_synth = synth_sizes[outlier_mask]
   outlier_real = real_sizes[outlier_mask]
   
   states = states[~outlier_mask]
   synth_sizes = synth_sizes[~outlier_mask]
   real_sizes = real_sizes[~outlier_mask]
   
   x = np.arange(len(states))
   
   plt.figure(figsize=(15, 7))
   
   plt.plot(x, synth_sizes, marker='o', linestyle='-', label='Synthetic Average', color='#4C72B0')
   plt.plot(x, real_sizes, marker='o', linestyle='-', label='Real Average', color='#55A868')
   
   if len(outlier_states) > 0:
       plt.figtext(0.02, 0.02, 
                  f'Outliers removed for scale:\n' + 
                  '\n'.join([f'{state}: Synth={synth:.1f}, Real={real:.1f}' 
                            for state, synth, real in zip(outlier_states, outlier_synth, outlier_real)]),
                  fontsize=10, ha='left', va='bottom')
   
   plt.title('Average Household Size by State: Synthetic vs Real', pad=20, fontsize=14)
   plt.xlabel('State', fontsize=12)
   plt.ylabel('Household Size', fontsize=12)
   plt.grid(True, linestyle='--', alpha=0.7)
   plt.legend(fontsize=10)
   
   plt.xticks(x, states, rotation=45, ha='right')
   
   plt.tight_layout()
   
   plt.savefig("household_plot", dpi=300, bbox_inches='tight')
   
   plt.close()

def main():
    # dense = get_zctas("https://localistica.com/usa/zipcodes/most-populated-zipcodes/")
    # sparse = get_zctas("https://localistica.com/usa/zipcodes/least-populated-zipcodes/")
    # random = get_random_zctas()
    
    # dense_aggregate = get_zcta_data(dense, "dense")
    # sparse_aggregate = get_zcta_data(sparse, "sparse")
    # random_aggregate = get_zcta_data(random, "random")

    df = pd.read_csv("~/censusdata/AgentTorch_SyntheticPopulation/analysis/random_aggregate.csv")

    state_averages = df.groupby('state').agg({
    'synth_avg_size': 'mean',
    'real_avg_size': 'mean'
    }).reset_index()

    result = list(zip(state_averages['state'], 
                    state_averages['synth_avg_size'], 
                    state_averages['real_avg_size']))

    plot_household_size(result)

    # dense_aggregate.to_csv("dense_aggregate.csv", index=False)
    # sparse_aggregate.to_csv("sparse_aggregate.csv", index=False)
    # random_aggregate.to_csv("random_aggregate.csv", index=False)

if __name__ == "__main__":  
    main()
