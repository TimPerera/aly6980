import pandas as pd
import openpyxl
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
c_handler = logging.StreamHandler()
c_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
c_handler.setFormatter(formatter)
logger.addHandler(c_handler)


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
    logger.debug(f'TIMES Data Rows: {len(times_data)}')
    # Fill in missing participant id 
    times_data['Participant ID'] = times_data['Participant ID'].fillna(method='ffill')
    times_data['Participant ID'] = times_data['Participant ID'].apply(lambda x: x.lower() if isinstance(x,str) else x)

    # Deal with missing Assessment Types
    times_df = times_data.groupby('Participant ID').apply(fix_assessment_type).reset_index(drop=True)
    
    return times_df

def clean_terminations(terminations_data):
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
    logger.debug(terminations_data.columns)

    # Forwardfill data
    terminations_data['Department'] = terminations_data['Department'].fillna(method='ffill')
    terminations_data['Program Name'] = terminations_data['Program Name'].fillna(method='ffill')
    terminations_data['Start Date'] = terminations_data['Start Date'].fillna(method='ffill')
    terminations_data['End Date'] = terminations_data['End Date'].fillna(method='ffill')
    terminations_data['Participant ID'] = terminations_data['Participant ID'].apply(lambda x: x.lower() if isinstance(x,str) else x).reset_index(drop=True)
    return terminations_data

def main():
   # Load data
    times_data = pd.read_excel('TIMES.xlsx')
    demographics_data = pd.read_excel('ParticipantDemographics.xlsx')
    initiations_data = pd.read_excel('ProgramInitiations.xlsx')
    terminations_data = pd.read_excel('ProgramTerminations.xlsx')
    servicedel_data = pd.read_excel('ServiceDeliveries.xlsx')

    # Run datasets through cleaning functions
    cleaned_times = clean_times(times_data)
    cleaned_terminations = clean_terminations(terminations_data)

    # Write cleaned data to files.
    cleaned_times.to_csv('output/transformed_times.csv', index=False)
    logger.debug('Wrote times data to Excel')
    cleaned_terminations.to_csv('output/transformed_terminations.csv', index=False)
    logger.debug('Wrote terminations data to Excel')


    #logger.debug(f'Final Df length:{len(times_demo_init_term_serv)}')

if __name__=='__main__':
    main()