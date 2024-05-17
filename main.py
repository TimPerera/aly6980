import pandas as pd

from utils import logger, write_to_csv, read_from_excel
from data_cleaning import clean_services, clean_terminations, clean_times, get_goal_setting_cols
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
 
    # Generate new data from services data for machine learning
    pivot_services_data = pivot_data(cleaned_services, goal_cols)
    delta_times_score = compute_delta_times(cleaned_times)
    # Join pivot_services and delta_times
    pivot_delta = pd.merge(pivot_services_data, delta_times_score, how='left',  left_index=True, right_index=True)
    pivot_delta.fillna('N/A - Missing TIMES Record', inplace=True) # Mark any row that doesn't have a Times Score as missing 
 
    # Write cleaned data to files.
    write_to_csv(cleaned_times, 'transformed_times.csv',logger=logger)
    write_to_csv(cleaned_terminations, 'transformed_terminations.csv',logger=logger)
    write_to_csv(cleaned_services,'transformed_services.csv',logger=logger)
    write_to_csv(pivot_services_data,'pivot_services.csv',logger=logger, index=True)
    write_to_csv(pivot_delta, 'model_data.csv',logger=logger, index=True)


if __name__=='__main__':
    file_names = ['TIMES.xlsx','ParticipantDemographics.xlsx','ProgramInitiations.xlsx',
                  'ProgramTerminations.xlsx','ServiceDeliveries.xlsx','Salesforce - Program & Service List.xlsx']
    main(file_names)
    