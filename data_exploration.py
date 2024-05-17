from utils import setup_logging, write_to_csv, read_from_excel
from data_cleaning import clean_services, clean_terminations, clean_times, get_goal_setting_cols
from data_transformations import pivot_data

logger = setup_logging()

def main(file_names):
   # Load data
    datasets = [read_from_excel(file_name) for file_name in file_names]
    times_data, demographics_data, initiations_data, terminations_data, servicedel_data, goal_setting_df =  datasets
    goal_cols = get_goal_setting_cols(goal_setting_df)
    
    # Run datasets through cleaning functions
    cleaned_times = clean_times(times_data)
    cleaned_terminations = clean_terminations(terminations_data)
    cleaned_services = clean_services(servicedel_data)
 
    # generate new data from services data for machine learning
    pivot_services_data = pivot_data(cleaned_services, goal_cols)
    
    # Write cleaned data to files.
    write_to_csv(cleaned_times, 'transformed_times.csv')
    write_to_csv(cleaned_terminations, 'transformed_terminations.csv')
    write_to_csv(cleaned_services,'transformed_services.csv')
    write_to_csv(pivot_services_data,'pivot_services.csv',index=True)


if __name__=='__main__':
    file_names = ['TIMES.xlsx','ParticipantDemographics.xlsx','ProgramInitiations.xlsx',
                  'ProgramTerminations.xlsx','ServiceDeliveries.xlsx','Salesforce - Program & Service List.xlsx']
    main(file_names)
    