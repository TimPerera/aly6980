import pandas as pd

def pivot_data(data, goal_setting_columns):
    """
    Pivots cleaned service deliverables data. Keeps only goal-setting related data. 

    """
    participant_data = {}
    for _, row in data.iterrows(): # Loop through dataset
        if row['Program Name'] in goal_setting_columns: # if program is goal-setting
            if row['Participant ID'] in participant_data: # if participant has already been registered before
                if row['Program Name'] in participant_data[row['Participant ID']]: # if participant has already engaged in program before.
                    participant_data[row['Participant ID']][row['Program Name']] += row['Quantity'] # Then add to existing quantity value
                else:
                    participant_data[row['Participant ID']][row['Program Name']] = row['Quantity'] # Then store quantity
            else: # this is a new participant there new program as well
                participant_data[row['Participant ID']] = {row['Program Name']: row['Quantity']}
    data_df = pd.DataFrame().from_dict(participant_data, orient='index').fillna(0)
    return data_df
