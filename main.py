"""
main.py 

Main logic of application. Loads, cleans, transforms, and writes data to CSV. 

"""
import pandas as pd

from utils import logger, write_to_file, read_from_excel
from data_cleaning import clean_services, clean_terminations, clean_times, get_goal_setting_cols, clean_demographics
from data_transformations import pivot_data, compute_delta_times

def main(file_names:list):
    """
    Main logic of application. Loads, cleans, transforms, and writes data to CSV. 

    Args:
        file_names (list): List of file names.
    
    Returns:
        None
    """
   # Load data
    datasets = [read_from_excel(file_name) for file_name in file_names]
    logger.debug(f'Collected {len(datasets)} datasets')
    times_data, demographics_data, initiations_data, terminations_data, servicedel_data, goal_setting_df =  datasets
    goal_cols = get_goal_setting_cols(goal_setting_df)
    
    # Run datasets through cleaning functions
    cleaned_times = clean_times(times_data)
    cleaned_terminations = clean_terminations(terminations_data)
    cleaned_services = clean_services(servicedel_data)
    cleaned_demographics = clean_demographics(demographics_data)

    # Generate new data from services data for machine learning
    pivot_services_data = pivot_data(cleaned_services, goal_cols)
    delta_times_score = compute_delta_times(cleaned_times) # returns three columns 'Delta TIMES', 'Initial TIMES','Last TIMES'
    # Join pivot_services and delta_times
    services_timesdelta = pd.merge(pivot_services_data, delta_times_score, how='left', left_index=True,right_index=True)
    cols_to_fill = ['Delta TIMES','Initial TIMES','Last TIMES']
    services_timesdelta.loc[:,cols_to_fill] = services_timesdelta.loc[:,cols_to_fill].fillna(-1)
    
    # Write cleaned data to files.
    write_to_file(cleaned_times, 'cleaned_times.xlsx',logger=logger)
    write_to_file(cleaned_terminations, 'cleaned_terminations.xlsx',logger=logger)
    write_to_file(cleaned_services,'cleaned_services.xlsx',logger=logger)
    write_to_file(cleaned_demographics, 'cleaned_demo_data.xlsx',logger=logger)
    write_to_file(pivot_services_data,'cleaned_and_transformed_services.xlsx',logger=logger, index=True)
    write_to_file(services_timesdelta, 'model_data.xlsx',logger=logger, index=True)



if __name__=='__main__':
    file_names = ['TIMES.xlsx','ParticipantDemographics.xlsx','ProgramInitiations.xlsx',
                  'ProgramTerminations.xlsx','ServiceDeliveries.xlsx','Salesforce - Program & Service List.xlsx']
    times = main(file_names)
    