"""
data_cleaning.py

Provides functions to clean sponsor data. This is a pre-processing step before transforming the data. See data_transformations.py.
"""
import pandas as pd
from datetime import datetime, timedelta

from utils import logger

def clean_services(data: pd.DataFrame)-> pd.DataFrame:
    """
    Cleans service deliveries data provided by sponsor. 

    Args:
        data (Dataframe): Unprocessed service deliveries data provided by sponsor. 

    Returns:
        data (Dataframe): Cleaned service deliveries dataset.
    
    """
    data = data.rename(columns={'Program: Program Name  ↑':'Program Name',
                                'Service: Service Name  ↑': 'Service Name'})
    data['Program Name'] = data['Program Name'].fillna(method='ffill')
    data['Service Name'] = data['Service Name'].fillna(method='ffill')
    data['Participant ID'] = data['Participant ID'].apply(lambda x: x.lower() if isinstance(x,str) else x)
    data = data[data['Participant ID'].apply(lambda x: isinstance(x, str))] # filters out unwanted values
    logger.debug(f'Before: {len(data)}')
    data = data[~data['Quantity'].apply(lambda x: pd.isna(x) or x==0)] # removes quantities where it either blank or 0.
    logger.debug(f'After: {len(data)}')
    return data

def clean_terminations(terminations_data: pd.DataFrame)-> pd.DataFrame:
    """
    Cleans terminations data provided by sponsor. 

    Args:
        terminations_data (Dataframe): Unprocessed terminations data provided by sponsor. 

    Returns:
        terminations (Dataframe): Cleaned terminations dataset.
    """
    # Rename columns
    terminations_data = terminations_data.rename(columns={'Department  ↑':'Department',
                                                          'Program Name  ↑':'Program Name',
                                                          'End Date  ↑': 'End Date'})
    terminations_data.drop(columns=['Unnamed: 3'],inplace=True)

    # Forwardfill data
    terminations_data['Department'] = terminations_data['Department'].fillna(method='ffill')
    terminations_data['Program Name'] = terminations_data['Program Name'].fillna(method='ffill')
    terminations_data['Start Date'] = terminations_data['Start Date'].fillna(method='ffill')
    terminations_data['End Date'] = terminations_data['End Date'].fillna(method='ffill')
    terminations_data['Participant ID'] = terminations_data['Participant ID'].apply(lambda x: x.lower() if isinstance(x,str) else x).reset_index(drop=True)
    return terminations_data

# Raw TIMES total Score/ (# of scored indicators * 5)
def calculate_scaled_times(rows):
    """
    Takes data from the times_data table and computes the scaled TIMES score using the formula: Raw TIMES total Score/ (# of scored indicators * 5)
    
    Returns scaled_times (float): Scaled TIMES Score.
    """
    # 
    if rows['TIMES Total Score'] == 0: return 0

    counter = rows[['Addiction','Family Structural Stability','Relationships','System Navigation','Employment Readiness','Employment Status', 'Economic Judgment', 'Economic Stability','Certification/Skills','Shelter','Safety','Self Awareness','Sense of Power','Nutrition'	,'Health',	'Mental Health','Spirituality',	'Values']].count()
    scaled_times = round(rows['TIMES Total Score']/(counter*5), 2)
    return scaled_times

def fix_assessment_type(group):
    """
    Defines rules for how to deal with missing or irregular Assessment Types
    1) First entry is Baseline
    2) Convert any later observations identified as a Baseline to Quarterly
    3) Any subsequent observations not labelled will be Quarterly

    Args:
        group (grouped dataframe): dataset grouped by Participant ID

    Returns:
        group (grouped dataframe): transformed grouped dataset. 

    """
    a_types = list(group['Assessment Type'])
    if pd.isna(a_types[0]): # if first entry is Null then make it Baseline
        a_types[0] = 'Baseline'
    if 'Baseline' in a_types[1:]: # Baseline entry exists after first entry. Replace that with 'Quarterly'
        idx = a_types[1:].index('Baseline')
        a_types[idx+1] = 'Quarterly'
    a_types = ['Quarterly' if pd.isna(a_type) else a_type for a_type in a_types] # if any further missing labels exist replace it with Quarterly

    current_date = datetime.now()
    if len(a_types)>1 and a_types[-1] != 'Closing':
        list(group['Assessment Date'])[-1] - current_date > timedelta(weeks=13.04) # Last assessment was more than 3 months ago. Change last entry to Closing
        a_types[-1] = 'Closing'

    
    group['Assessment Type'] = a_types
    return group

def clean_times(times_data):
    """
    Function designed to resolve any data issues found in times.xlsx. 
    Args:
        times_data (Dataframe): Unprocessed times dataframe provided by client.

    Returns:
        times (Dataframe): Cleaned times dataframe.
    """
     # Calculate Scaled TIMES Score Column
    scaled_times = times_data.apply(calculate_scaled_times, axis=1).reset_index(drop=True)
    times_data.insert(3, 'Scaled TIMES Score',scaled_times)
    times_data = times_data.rename(columns={'Participant: Participant ID  ↑':'Participant ID',
                                            'Assessment Completed Date  ↑':'Assessment Date'})
    times_data['Assessment Date'] = pd.to_datetime(times_data['Assessment Date'],format='%Y-%m-%d')
    # Fill in missing participant id 
    times_data['Participant ID'] = times_data['Participant ID'].fillna(method='ffill')
    times_data['Participant ID'] = times_data['Participant ID'].apply(lambda x: x.lower() if isinstance(x,str) else x)
    # Deal with missing Assessment Types
    times_df = times_data.groupby('Participant ID').apply(fix_assessment_type).reset_index(drop=True)
    return times_df

def get_goal_setting_cols(df: pd.DataFrame)-> list:
    """
    Retrieves programs and services that are defined as "Goal Setting" from supplied df.

    df (pd.DataFrame): Salesforce - Program & Service List.xlsx file containing all programs and services
                       provided by sponsor.

    goal_setting_columns (list): 
    """
    goal_setting_columns = []
    for _, row in df.iterrows():
        if row['GOAL-SETTING'] == 'y':
            goal_setting_columns.append(row['PROGRAM'])
    return goal_setting_columns


if __name__ == '__main__':
    service_data = pd.read_excel('ServiceDeliveries.xlsx')
    clean_service = clean_services(service_data)
    logger.debug(clean_service['Quantity'][:10])
    #clean_service.to_csv('faulty.csv')
    # logger.debug(f'Len of df is {len(clean_service)}')
    # #df = clean_service[~pd.isna(clean_service['Participant ID'])]
    # df = clean_service[clean_service['Participant ID'].apply(lambda x: isinstance(x, str))]
    # logger.debug(df['Participant ID'])
    # df.to_csv('sample.csv')

    # programs = pd.read_excel('Salesforce - Program & Service List.xlsx')
    # logger.debug(get_goal_setting_cols(programs))
