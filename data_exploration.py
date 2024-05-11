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

def main():
    # Calculate Scaled TIMES Score Column
    times_data = pd.read_excel('TIMES.xlsx')
    demographics_data = pd.read_excel('ParticipantDemographics.xlsx')
    initiations_data = pd.read_excel('ProgramInitiations.xlsx')
    terminations_data = pd.read_excel('ProgramTerminations.xlsx')
    servicedel_data = pd.read_excel('ServiceDeliveries.xlsx')
    scaled_times = times_data.apply(calculate_scaled_times, axis=1).reset_index(drop=True)
    times_data.insert(3, 'Scaled TIMES Score',scaled_times)
    times_data = times_data.rename(columns={'Participant: Participant ID  ↑':'Participant ID',
                                            'Assessment Completed Date  ↑':'Assessment Date'})
    
    times_data['Assessment Date'] = pd.to_datetime(times_data['Assessment Date'],format='%Y-%m-%d')
    logger.debug(f'TIMES Data Rows: {len(times_data)}')
    # Fill in missing participant id 
    times_data['Participant ID'] = times_data['Participant ID'].fillna(method='ffill')

    # Deal with missing Assessment Types
    times_df = times_data.groupby('Participant ID').apply(fix_assessment_type).reset_index(drop=True)
    times_df.to_csv('transformed_times.csv')
    # Left Join on All other data sets
    #times_demo = pd.merge(times_df, demographics_data, on='Participant ID',how='left')
    # times_demo_init = pd.merge(times_df, initiations_data, on = 'Participant ID', how='left')
    # times_demo_init_term = pd.merge(times_demo_init, terminations_data, on='Participant ID',how='left')
    # times_demo_init_term_serv = pd.merge(times_demo_init_term, servicedel_data, on='Participant ID',how='left')

    #logger.debug(f'Final Df length:{len(times_demo_init_term_serv)}')

    #times_demo_init_term_serv.to_csv('transformed_data.csv')

if __name__=='__main__':
    main()