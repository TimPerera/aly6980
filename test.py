import pandas as pd

times = pd.read_excel('TIMES.xlsx')
participant = times['Participant: Participant ID  ↑'][3]

demo = pd.read_excel('ParticipantDemographics.xlsx')

res = demo[demo['Participant ID']==participant]

print(res)