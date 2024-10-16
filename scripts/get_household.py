import requests
import pandas as pd
from constants import API_KEY, STATE_DICT, AGE_GROUP_MAPPING
from get_population import fetch_census_data, get_state_zctas
import pickle

def get_household_labels():
    variable_labels = {
        "B11001_001E": "Households",
        "B11001_002E": "Family Households",
        "B11001_007E": "Nonfamily Households",
        "B25010_001E": "Average Household Size"
    }

    return list(variable_labels.keys()), variable_labels

def process_household_data(data, labels, state_abbr, zcta):
    df = pd.DataFrame(data[1:], columns=data[0])
    area = state_abbr + '0001'
    
    household_data = {
        'area': area,
    }
    
    for variable, label in labels.items():
        value = float(df.loc[0, variable])
        if label == 'Households':
            household_data['household_num'] = int(value)
        elif label == 'Family Households':
            household_data['family_households'] = int(value)
        elif label == 'Nonfamily Households':
            household_data['nonfamily_households'] = int(value)
        elif label == 'Average Household Size':
            household_data['average_household_size'] = value
    
    age_gender_df = pd.read_csv('data/age_gender/' + state_abbr + zcta + '_age_gender.csv')
    # household_data['people_num'] = age_gender_df['count'].sum()
    # household_data['children_num'] = age_gender_df[age_gender_df['age'] == 'U5']['count'].sum()
    household_data['children_num'] = age_gender_df[age_gender_df['age'].isin(AGE_GROUP_MAPPING['children_list'])]['count'].sum()
    household_data['people_num'] = age_gender_df[age_gender_df['age'].isin(AGE_GROUP_MAPPING['adult_list'])]['count'].sum()
    
    result_df = pd.DataFrame([household_data])
    
    return result_df

def main():
    # get household labels and data
    variables, variable_labels = get_household_labels()

    # get state zctas
    state = "36"
    state_abbr = "NY"
    state_zctas = get_state_zctas(state)

    for zcta in state_zctas:
        try:
            data = fetch_census_data(variables, zcta)

            # get df
            household_df = process_household_data(data, variable_labels, state_abbr, zcta)

            # save
            household_df.to_pickle('data/household/' + state_abbr + zcta + '_household.pkl')

        except:
            continue
    
if __name__ == "__main__":
    main()

