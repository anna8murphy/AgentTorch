import requests
import pandas as pd
from constants import API_KEY, STATE_DICT
from get_population import fetch_census_data
import pickle

def get_household_labels():
    variable_labels = {
        "B11001_001E": "Households",
        "B11001_002E": "Family Households",
        "C11016_007E": "Nonfamily Households",
        "B25010_001E": "Average Household Size"
    }

    return list(variable_labels.keys()), variable_labels

def process_household_data(data, labels, state_abbr):
    df = pd.DataFrame(data[1:], columns=data[0])
    area = state_abbr + '0001'
    
    household_data = {
        'area': area,
    }
    
    for variable, label in labels.items():
        value = float(df.loc[0, variable])
        if label == 'Households':
            household_data['household_num'] = value
        elif label == 'Family Households':
            household_data['family_households'] = value
        elif label == 'Nonfamily Households':
            household_data['nonfamily_households'] = value
        elif label == 'Average Household Size':
            household_data['average_household_size'] = float(value)
    
    # Read STATE_age_gender.csv to get People and Children count
    age_gender_df = pd.read_csv('data/age_gender/' + state_abbr + '_age_gender.csv')
    household_data['people_num'] = age_gender_df['count'].sum()
    household_data['children_num'] = age_gender_df[age_gender_df['age'] == 'U5']['count'].sum()
    
    result_df = pd.DataFrame([household_data])
    
    return result_df

def main():
    # get household labels and data
    variables, variable_labels = get_household_labels()
    
    for state in STATE_DICT:
        s = str(state)
        state_abbr = STATE_DICT[state][1]

        data = fetch_census_data(variables, s)

        # get df
        household_df = process_household_data(data, variable_labels, state_abbr)

        # save
        household_df.to_pickle('data/household/' + state_abbr + '_household.pkl')
    
if __name__ == "__main__":
    main()

