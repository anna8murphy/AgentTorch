import pandas as pd
import numpy as np
import scipy.stats as stats
import pickle
import matplotlib.pyplot as plt
import seaborn as sns
import pingouin as pg

def load_data(synthetic_pop, real_pop):
    """
    Aggregates individual synthetic data to match real data format, and maps age groups to numeric values.

    Parameters:
    synthetic_pop (str): Path to synthetic population data
    real_pop (str): Path to real population data

    Returns:
    pandas.DataFrame: DataFrames containing synthetic and real population data
    """
    with open(synthetic_pop, 'rb') as file:
        synthetic_data = pickle.load(file)
    
    with open(real_pop, 'rb') as file:
        real_data = pickle.load(file)
    
    age_mapping = {
        'u5': 2.5, '5t9': 7, '10t14': 12, '15t17': 16, '18t19': 18.5, '20t21': 20.5, '22t24': 23,
        '25t29': 27, '30t34': 32, '35t39': 37, '40t44': 42, '45t49': 47, '50t54': 52, '55t59': 57,
        '60t61': 60.5, '62t64': 63, '65t66': 65.5, '67t69': 68, '70t74': 72, '75t79': 77, '80t84': 82, '85plus': 87
    }
    
    age_gender_counts = (
        synthetic_data.groupby(['area', 'gender', 'age', 'region'])
        .size()
        .reset_index(name='count')
    )
    
    synthetic = age_gender_counts
    synthetic['age_numeric'] = synthetic['age'].map(age_mapping)
    
    real = real_data['age_gender']
    real['age_numeric'] = real['age'].map(age_mapping)
    
    return synthetic, real

def compare_median_age(synthetic, real):
    """
    Compares median age between synthetic and real dataset.

    Parameters: 
    synthetic (pandas.DataFrame): Synthetic population data
    real (pandas.DataFrame): Real population data

    Returns:
    dict: Dictionary containing median age values
    """
    median_age_df = pd.DataFrame({
        "synthetic": synthetic.groupby('gender')['age_numeric'].median(),
        "real": real.groupby('gender')['age_numeric'].median()
    })

    median_age_dict = {
        "synthetic_male": median_age_df.loc['male', 'synthetic'] if 'male' in median_age_df.index else None,
        "synthetic_female": median_age_df.loc['female', 'synthetic'] if 'female' in median_age_df.index else None,
        "real_male": median_age_df.loc['male', 'real'] if 'male' in median_age_df.index else None,
        "real_female": median_age_df.loc['female', 'real'] if 'female' in median_age_df.index else None
    }

    return median_age_dict
    
def get_total_pop(synthetic, real):
    """
    Get total population from synthetic and real demographic datasets.
    """
    total_synthetic = synthetic['count'].sum()
    total_real = real['count'].sum()

    return total_synthetic, total_real

def viz_age_distributions(synthetic_df, real_df):
    """
    Visualize age distributions for synthetic and real data.
    """
    synth_by_age = synthetic_df.sort_values('age_numeric')
    real_by_age = real_df.sort_values('age_numeric')
    
    plt.figure(figsize=(12, 6))
    
    plt.plot(synth_by_age['age'], synth_by_age['count'], 
             marker='o', linestyle='-', label='Synthetic', color='#4C72B0')
    plt.plot(real_by_age['age'], real_by_age['count'], 
             marker='o', linestyle='-', label='Real', color='#55A868')
    
    plt.title('Age Distribution Comparison', pad=20, fontsize=14)
    plt.xlabel('Age Group', fontsize=12)
    plt.ylabel('Count', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend(fontsize=10)
    
    plt.xticks(rotation=45, ha='right')
    
    plt.tight_layout()
    
    plt.savefig("analysis/sample_age_plot", dpi=300, bbox_inches='tight')
    
    plt.close()

def chi_square_test(synthetic, real):
    """
    Performs chi-square test on both synthetic and real data to determine if age and gender distributions are likely to be independent.
    """
    synthetic_gender_age_table = pd.crosstab(synthetic['gender'], synthetic['age'])
    real_gender_age_table = pd.crosstab(real['gender'], real['age'])
    
    chi2_synthetic, p_synthetic, dof_synthetic, expected_synthetic = stats.chi2_contingency(synthetic_gender_age_table)
    chi2_real, p_real, dof_real, expected_real = stats.chi2_contingency(real_gender_age_table)

    return chi2_synthetic, p_synthetic, chi2_real, p_real

def mann_whitney_u_test(synthetic, real):
    """
    Performs Mann-Whitney U test on both synthetic and real data to determine if age distributions across gender are similar between the datasets.
    """
    real_male = np.repeat(real.loc[real['gender'] == 'male', 'age_numeric'], real.loc[real['gender'] == 'male', 'count'])
    real_female = np.repeat(real.loc[real['gender'] == 'female', 'age_numeric'], real.loc[real['gender'] == 'female', 'count'])
    
    synth_male = np.repeat(synthetic.loc[synthetic['gender'] == 'male', 'age_numeric'], synthetic.loc[synthetic['gender'] == 'male', 'count'])
    synth_female = np.repeat(synthetic.loc[synthetic['gender'] == 'female', 'age_numeric'], synthetic.loc[synthetic['gender'] == 'female', 'count'])

    u_stat_m, p_val_m = stats.mannwhitneyu(real_male, synth_male, nan_policy='omit')
    u_stat_f, p_val_f = stats.mannwhitneyu(real_female, synth_female, nan_policy='omit')

    return u_stat_m, p_val_m, u_stat_f, p_val_f

def main():

    # density = "sparse"
    density = "dense"

    if density == "sparse":
        synthetic_pop = 'output/population/RI/02873_base_population.pkl'
        real_pop = 'zcta_data/population/RI/02873_population.pkl'

    else: # dense
        synthetic_pop = 'output/population/NY/10001_base_population.pkl'
        real_pop = 'zcta_data/population/NY/10001_population.pkl'
    
    synthetic, real = load_data(synthetic_pop, real_pop)
    viz_age_distributions(synthetic, real)
    
    # print(compare_median_age(synthetic, real))

if __name__ == "__main__":
    main()