"""
Ad hoc requests - after the data cleaning was completed, we realized we needed further data transformations. 
To maintain organization and readability of code, I've put those requests into this module. 
"""
import pandas as pd

from utils import logger, write_to_file


def survey_enumerator(grouped_data):
    """
    Enumerates the surveys in the order they are observed in the source dataset.
    Args:
        grouped_data: transformed_times.xlsx data grouped by Participant ID.

    Returns:

    """
    grouped_data['Assesment Type Enumerated'] = ['Survey ' + str(x) for x in range(1, len(grouped_data)+1)]
    return grouped_data

def main():
    # Wan's Request - enumerate surveys under assessment type.
    transformed_times = pd.read_excel('output/cleaned_times.xlsx',header=0)
    transformed_times = transformed_times.groupby('Participant ID').apply(survey_enumerator, include_groups=False)

    # Adaire's Request 
    # merge demo data and model data
    model_data = pd.read_excel('output/model_data.xlsx',header=0)
    demo_data = pd.read_excel('output/cleaned_demo_data.xlsx', header=0)
    merged_md_file = model_data.merge(demo_data, how='left',left_index=True,right_index=True)

    # Write solutions to file
    write_to_file(transformed_times,'wan_request.xlsx',logger=logger)
    write_to_file(merged_md_file,'adaire_request.xlsx',logger=logger)


if __name__=='__main__':
    main()
    