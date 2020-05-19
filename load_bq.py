
from google.cloud import storage
import pandas as pd
import pandas_gbq

client = storage.Client()

schema =  [ 
  {
    "mode": "NULLABLE", 
    "name": "geo_value", 
    "type": "STRING"
  }, 
  {
    "mode": "NULLABLE", 
    "name": "time_value", 
    "type": "STRING"
  }, 
  {
    "mode": "NULLABLE", 
    "name": "direction", 
    "type": "FLOAT64"
  }, 
  {
    "mode": "NULLABLE", 
    "name": "value", 
    "type": "FLOAT64"
  }, 
  {
    "mode": "NULLABLE", 
    "name": "stderr", 
    "type": "FLOAT64"
  }, 
  {
    "mode": "NULLABLE", 
    "name": "sample_size", 
    "type": "FLOAT64"
  }

]

col = [
'geo_value',
'time_value',
'direction',
'value',
'stderr',
'sample_size',]

def loadFile(data, context):
    bucket = client.get_bucket(data['bucket'])

    dest_file = '/tmp/file.csv'
    tables = {'doctor-visits_smoothed_adj_cli':'delphi_cmu1.doctor_visits_smoothed_adj_cli',
    'doctor-visits_smoothed_cli':'delphi_cmu1.doctor_visits_smoothed_cli',
    'fb-survey_raw_cli':'delphi_cmu1.fb_survey_raw_cli',
    'fb-survey_raw_wcli':'delphi_cmu1.fb_survey_raw_wcli',
    'google-survey_raw_cli':'delphi_cmu1.google_survey_raw_cli',
    'google-survey_raw_smoothed_cli':'delphi_cmu1.google_survey_smoothed_cli',
    'ght_raw_search':'delphi_cmu1.ght_raw_search',
    'ght_smoothed_search':'delphi_cmu1.ght_smoothed_search',
    'quidel_raw_pct_negative':'delphi_cmu1.quidel_raw_pct_negative',
    'quidel_smoothed_pct_negative':'delphi_cmu1.quidel_smoothed_pct_negative',
    'quidel_raw_tests_per_device':'delphi_cmu1.quidel_raw_tests_per_device',
    'quidel_smoothed_tests_per_device':'delphi_cmu1.quidel_smoothed_tests_per_device',
    'jhu-csse_confirmed_cumulative_num':'delphi_cmu1.jhu_csse_confirmed_cumulative_num',
    'jhu-csse_deaths_cumulative_num':'delphi_cmu1.jhu_csse_deaths_cumulative_num',  
    'jhu-csse_confirmed_incidence_num':'delphi_cmu1.jhu_csse_confirmed_incidence_num',
    'jhu-csse_deaths_incidence_num':'delphi_cmu1.jhu_csse_deaths_incidence_num',  
    'jhu-csse_confirmed_incidence_prop':'delphi_cmu1.jhu_csse_confirmed_incidence_prop',
    'jhu-csse_deaths_incidence_prop':'delphi_cmu1.jhu_csse_deaths_incidence_prop'
    }
    dest_table=''
    for k, v in tables.items():
        if k in data['name']:
            dest_table=v
            blob = bucket.blob(data['name'])
    if dest_table=='':
        return    
    
    blob.download_to_filename(dest_file)
    newdata = pd.read_csv(dest_file)
    #newdata['time_value'] = pd.to_datetime(newdata['time_value'], format = "%Y%m%d")
    newdata[['geo_value']] = newdata[['geo_value']].applymap(lambda x: str(x))
    newdata[['value','stderr', 'sample_size','direction']] = newdata[['value','stderr', 'sample_size','direction']].applymap(lambda x: float(x))
    data=newdata[col]
    print(data.info())
    data.to_gbq(destination_table=dest_table,
    project_id='covid-19-270817',
    if_exists='append', 
    table_schema = schema
    )

    #print(f"Processing file: {file['name']}")
    return
