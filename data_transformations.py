"""
data_transformations.py

This module describes the functions applied to sponsor's dataset (after cleaning process - check data_cleaning.py)
"""

import pandas as pd
from utils import logger

def pivot_data(data: pd.DataFrame, goal_setting_columns: list)-> pd.DataFrame:
    """
    Pivots cleaned service deliverables data. Keeps only goal-setting related data. 

    """
    participant_data = {}
    for _, row in data.iterrows(): # Loop through dataset
        if row['Program Name'] in goal_setting_columns: # if program is goal-setting
            if row['Participant ID'] in participant_data: # if participant has already been registered before
                if f"{row['Program Name']} - {row['Unit of Measurement']}" in participant_data[row['Participant ID']]: # if participant has already engaged in program before.
                    participant_data[row['Participant ID']][f"{row['Program Name']} - {row['Unit of Measurement']}"] += row['Quantity'] # Then add to existing quantity value
                else:
                    participant_data[row['Participant ID']][f"{row['Program Name']} - {row['Unit of Measurement']}"] = row['Quantity'] # Then store quantity
            else: # this is a new participant there new program as well
                participant_data[row['Participant ID']] = {f"{row['Program Name']} - {row['Unit of Measurement']}": row['Quantity']}
    data_df = pd.DataFrame().from_dict(participant_data, orient='index').fillna(0)
    return data_df

def compute_delta_times(data:pd.DataFrame)-> pd.DataFrame:
    """
    As we need to consolidate information for each participant to one row, this function provides the
    first and last Scaled TIMES score, and the difference (delta) between the two.

    Args:
        data (pd.DataFrame): cleaned/transformed TIMES data. 

    Returns:
        df (pd.DataFrame): Dataframe containing scaled Delta TIMES, Initial TIMES, and Last TIMES 
    """
    res = {}
    for name, group in data.groupby('Participant ID'):
        if len(group) > 1:
            times_list = list(group['Scaled TIMES Score'])
            delta_times = times_list[-1] - times_list[0]
            res[name] = [delta_times, times_list[0], times_list[-1]]
    df = pd.DataFrame().from_dict(res, orient='index')
    df.columns = ['Delta TIMES', 'Initial TIMES','Last TIMES']
    return df


    
if __name__=='__main__':
    # data = pd.read_csv('output/transformed_services.csv')
    # goal_cols_df = pd.read_excel('Salesforce - Program & Service List.xlsx')
    # goal_cols = get_goal_setting_cols(goal_cols_df)
    # pivot_data(data, goal_cols)
    data = pd.read_csv('output/transformed_times.csv')
    res = compute_delta_times(data.iloc[:15])
    logger.debug(res)