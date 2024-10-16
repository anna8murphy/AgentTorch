import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import pickle
import json
from constants import STATE_DICT, API_KEY, AGE_PATTERNS

def fetch_url(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.text

def get_age_gender_labels(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.find('table')
    if not table:
        print("No table found in the HTML content")
        return [], {}
    
    tbody = table.find('tbody')
    
    variables = []
    variable_labels = {}
    for row in tbody.find_all('tr'):
        cells = row.find_all('td')
        if len(cells) >= 3:
            variable_name = cells[0].text.strip()
            label = cells[1].text.strip()
            concept = cells[2].text.strip()

            if concept == "Sex by Age":
                variables.append(variable_name)
                variable_labels[variable_name] = label
    
    return variables, variable_labels

def get_ethnicity_labels():
    variable_labels = {
        "B02001_002E": "White",
        "B02001_003E": "Black",
        "B02001_004E": "American Indian and Alaska Native",
        "B02001_005E": "Asian",
        "B02001_006E": "Native Hawaiian and Other Pacific Islander",
        "B02001_007E": "Some Other Race",
        "B02001_008E": "Two or More Races"
    }
    return list(variable_labels.keys()), variable_labels

def fetch_census_data(variables, zcta):
    base_url = "https://api.census.gov/data/2022/acs/acs5"
    
    get_variables = "NAME," + ",".join(variables)
    
    params = {
        "get": get_variables,
        "for": f"zip code tabulation area:{zcta}",
        "key": API_KEY
    }
    
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    return response.json()

def parse_label(label):
    gender = 'male' if 'Male' in label else 'female'
    
    for pattern, age_group in AGE_PATTERNS:
        if re.search(pattern, label):
            return gender, age_group
    
    return gender, 'unknown'

def process_age_gender_data(data, variable_labels, state_abbr):
    df = pd.DataFrame(data[1:], columns=data[0])
    
    region = state_abbr
    area = region + '0001'
    
    transformed_data = {}
    
    for variable, value in df.iloc[0].items():
        if variable in variable_labels:
            label = variable_labels[variable]
            gender, age = parse_label(label)
            count = int(value)
            
            key = (area, gender, age, region)
            if key[2] != 'unknown':
                if (key[2] == '20' or key[2] == '21'):
                    key = (area, gender, '20t21', region)
                if key not in transformed_data:
                    transformed_data[key] = 0
                transformed_data[key] += count
    
    result = [
        {
            'area': area,
            'gender': gender,
            'age': age,
            'count': count,
            'region': region
        }
        for (area, gender, age, region), count in transformed_data.items()
    ]
    return pd.DataFrame(result)

def process_ethnicity_data(data, labels, state_abbr):
    df = pd.DataFrame(data[1:], columns=data[0])
    id_vars = ['NAME', 'zip code tabulation area']
    value_vars = list(labels.keys())
    melted_df = df.melt(id_vars=id_vars, value_vars=value_vars, var_name='variable', value_name='count')
    
    melted_df['ethnicity'] = melted_df['variable'].map(labels)
    melted_df['area'] = state_abbr + '0001'
    melted_df['region'] = state_abbr
    
    result_df = melted_df[['area', 'ethnicity', 'count', 'region']]
    
    result_df['count'] = result_df['count'].astype(int)
    
    return result_df

def get_state_zctas(state):
    zcta_url = "https://api.census.gov/data/2017/acs/acs5?get=NAME,group(B19013)&for=zip%20code%20tabulation%20area:*" # get all zctas
    zcta_response = fetch_url(zcta_url)
    split_response = zcta_response.split('],')

    state_zctas = [line.split(',')[-1][1:-1] for line in split_response[1:] if line.split(',')[-2] == f'"{state}"']
    return state_zctas


def main():
    state = "36"
    state_abbr = "NY"

    # get zctas
    state_zctas = get_state_zctas(state)
    
    # get census variables
    census_vars_url = "https://api.census.gov/data/2023/acs/acs1/variables.html"
    html_content = fetch_url(census_vars_url)
    age_gender_vars, age_gender_labels = get_age_gender_labels(html_content)
    ethnicity_vars, ethnicity_labels = get_ethnicity_labels()

    for zcta in state_zctas:

        try:
            # fetch data using api
            age_gender_data = fetch_census_data(age_gender_vars, zcta)
            ethnicity_data = fetch_census_data(ethnicity_vars, zcta)

            # create dataframes
            age_gender_df = process_age_gender_data(age_gender_data, age_gender_labels, state_abbr)
            ethnicity_df = process_ethnicity_data(ethnicity_data, ethnicity_labels, state_abbr)

            # create dict of dataframes
            population_data = {
            'age_gender': age_gender_df,
            'ethnicity': ethnicity_df
            }

            # store age and gender data, used in household.py
            filename = 'data/age_gender/' + state_abbr + zcta + '_age_gender.csv'
            age_gender_df.to_csv(filename, index=False)

            # write combined population data to pickle   
            filename = 'data/population/' + state_abbr + zcta + '_population_data.pkl'
            with open(filename, 'wb') as file:
                pickle.dump(population_data, file)
            print("saved data for zcta", zcta)
            
        except:
            print("error processing zcta", zcta)
            continue

if __name__ == "__main__":
    main()