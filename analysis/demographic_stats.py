import pandas as pd
import numpy as np
import scipy.stats as stats
import pickle
import matplotlib.pyplot as plt
import seaborn as sns
import pingouin as pg


def load_data(synthetic_pop, real_pop):
    with open(synthetic_pop, 'rb') as file:
        synthetic_data = pickle.load(file)
    
    with open(real_pop, 'rb') as file:
        real_data = pickle.load(file)
    
    # standardize format - aggregate data
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
    median_age_df = pd.DataFrame({
        "synthetic": synthetic.groupby('gender')['age_numeric'].median(),
        "real": real.groupby('gender')['age_numeric'].median()
    })
    
    print("median age by gender:")
    print(median_age_df)

def visualize_age_distributions(synthetic, real):
    fig, axes = plt.subplots(1, 2, figsize=(12, 6))
    
    axes[0].set_title("synthetic data age distribution")
    synthetic.groupby('age_numeric')['count'].sum().sort_index().plot(kind='bar', ax=axes[0])
    axes[0].set_xlabel("age")
    axes[0].set_ylabel("count")
    
    axes[1].set_title("real data age distribution")
    real.groupby('age_numeric')['count'].sum().sort_index().plot(kind='bar', ax=axes[1])
    axes[1].set_xlabel("age")
    axes[1].set_ylabel("count")
    
    plt.tight_layout()
    plt.savefig("age_distributions.png")

def chi_square_test(synthetic, real):
    synthetic_gender_age_table = pd.crosstab(synthetic['gender'], synthetic['age'])
    real_gender_age_table = pd.crosstab(real['gender'], real['age'])
    
    chi2_synthetic, p_synthetic, dof_synthetic, expected_synthetic = stats.chi2_contingency(synthetic_gender_age_table)
    chi2_real, p_real, dof_real, expected_real = stats.chi2_contingency(real_gender_age_table)
    
    print("\nchi-square for synthetic data:")
    print(f"chi2: {chi2_synthetic}, p-value: {p_synthetic}")
    
    print("\nchi-square for real data:")
    print(f"chi2: {chi2_real}, p-value: {p_real}")

def mann_whitney_u_test(synthetic, real):
    real_male_ages = np.repeat(real.loc[real['gender'] == 'male', 'age_numeric'], real.loc[real['gender'] == 'male', 'count'])
    real_female_ages = np.repeat(real.loc[real['gender'] == 'female', 'age_numeric'], real.loc[real['gender'] == 'female', 'count'])
    
    synthetic_male_ages = np.repeat(synthetic.loc[synthetic['gender'] == 'male', 'age_numeric'], synthetic.loc[synthetic['gender'] == 'male', 'count'])
    synthetic_female_ages = np.repeat(synthetic.loc[synthetic['gender'] == 'female', 'age_numeric'], synthetic.loc[synthetic['gender'] == 'female', 'count'])

    print("\nmann whitney u test (males):")
    u_statistic, p_value = stats.mannwhitneyu(real_male_ages, synthetic_male_ages, nan_policy='omit')
    print(f"u-statistic: {u_statistic}", "n1*n2=", len(real_male_ages)*len(synthetic_male_ages))
    print(f"p-value: {p_value}")
    
    print("\nmann whitney u test (females):")
    u_statistic, p_value = stats.mannwhitneyu(real_female_ages, synthetic_female_ages, nan_policy='omit')
    print(f"u-statistic: {u_statistic}", "n1*n2=", len(real_female_ages)*len(synthetic_female_ages))
    print(f"p-value: {p_value}")

def main():

    density = "sparse"

    if density == "sparse":
        synthetic_pop = 'output/population/RI/02873_base_population.pkl'
        real_pop = 'zcta_data/population/RI/02873_population.pkl'

    else: # dense
        synthetic_pop = 'output/population/NY/10001_base_population.pkl'
        real_pop = 'zcta_data/population/NY/10001_population.pkl'
    
    synthetic, real = load_data(synthetic_pop, real_pop)
    print(density)
    
    compare_median_age(synthetic, real)
    visualize_age_distributions(synthetic, real)
    chi_square_test(synthetic, real)
    mann_whitney_u_test(synthetic, real) # sparse has higher p-val

if __name__ == "__main__":
    main()