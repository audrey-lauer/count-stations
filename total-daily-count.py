import numpy as np
import pandas as pd
import os
import glob

meteo_variable = {
  'temperature':'12004',
  'dewpoint':   '12006',
  'wind_dir':   '11011',
  'wind_speed': '11012',
  'pressure':   '10004',
  'precip_01h': '13019',
  'precip_03h': '13020',
  'precip_06h': '13021',
  'precip_12h': '13022',
  'precip_24h': '13023',
  'snow_dept':  '13013'
}

year_list = np.arange(1993, 2002, 1)
year_list = [str(y) for y in year_list]

obs_type = 'from-gerard'

for year in year_list:
    # Create dataframe with all dates in year
    date_range = pd.date_range(year+'0101',year+'1231',freq='d')
    count = pd.DataFrame(columns=meteo_variable.keys())
    count['date'] = date_range
    
    # 1 file per day for ISD
    if obs_type == 'isd':

        input_path = '/home/mbu001/ss6/data_observations/CSV/ISD/'+year+'/'
        all_files = glob.glob(os.path.join(input_path, "*.csv"))

        for filename in all_files:
            # Open file
            df = pd.read_csv(filename)
         
            # Find date
            date = int(df['YYYMMDD'][0])
            date = pd.to_datetime(str(date), format='%Y-%m-%d')
         
            # Count number of records
            for variable in meteo_variable.keys():
                count[variable].loc[count['date'] == date] = (df[meteo_variable[variable]].values >= 0).sum()

    # 1 file per month
    elif obs_type == 'isd-old' or obs_type == 'ade' or obs_type == 'from-gerard':

        input_path = '/home/aul001/reanalyse/validation-v3/count-stations/data/burp/'+obs_type+'/temp/'

        for month in ['01','02','03','04','05','06','07','08','09','10','11','12']:
            df = pd.read_pickle(input_path + 'data-'+year+'-'+month+'.pkl')

            date_range2 = df['date'].to_list() 
            date_range2 = list(set(date_range2))

            for date in date_range2:
                try:
                    if int(year) < 2000:
                        date2 = pd.to_datetime('19'+str(date), format='%Y-%m-%d')
                    elif int(year) >= 2000:
                        date2 = pd.to_datetime(str(date), format='%Y-%m-%d')

                    df_temp = df.loc[df['date'] == date]
    
                    # Count number of records
                    for variable in meteo_variable.keys():
                        df_temp2 = df_temp.loc[ df_temp['e_bufrid'] == int(meteo_variable[variable]) ]
                        
                        if obs_type == 'ade':
                            df_temp2 = df_temp2.loc[ df_temp2['idtyp'] != 13 ] # Enlever stations SHIP
                            df_temp2 = df_temp2.loc[ df_temp2['idtyp'] != 147] # Enlever stations ASHIP

                        count[variable].loc[count['date'] == date2] = (df_temp2['rval'].values >= 0).sum()

                except:
                    continue

    count = count.sort_values('date')
    print(count)
    count.to_csv('data/number-of-stations/'+obs_type+'/'+year+'-number-of-stations.csv', index=False)

























