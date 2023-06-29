import numpy as np
import pandas as pd
import os
import glob
from burp_reader import BurpFileReader
import timeit
from multiprocessing import Pool

def read_data(year, month):

    isd_or_ade = 'hadisd'

    if isd_or_ade == 'isd':
        input_path = '/home/mbu001/ss6/data_observations/BURP/ISD/v2_nonfiltered/'+year+'/all'
    elif isd_or_ade == 'ade':
        input_path = '/home/smco813/ss5/data_obs/rarc2/link2burp4allyears/'
    elif isd_or_ade == 'from-gerard':
        input_path = '/home/aul001/dd/data/obs-from-gerard'
    elif isd_or_ade == 'hadisd':
        input_path = '/home/aul001/dd/data/obs/hadISD/burp/surface/synop/'

    meteo_variable = {
      'temperature':12004.0,
      'dewpoint':   12006.0,
      'precip_01h': 13019.0,
      'precip_03h': 13020.0,
      'precip_06h': 13021.0,
      'precip_12h': 13022.0,
      'precip_24h': 13023.0#,
      #'snow_depth': 13013.0
    }

    all_files = glob.glob(os.path.join(input_path, year+month+"*"))
    #all_files = glob.glob(os.path.join(input_path+'/s*/*/',year+month+"*"))
    #all_files = glob.glob(os.path.join(input_path+'/s*/',year+month+"*"))
    all_files.sort()
    #all_files = all_files[0:10]

    df_data = pd.DataFrame()
    # Merge all observations of the month
    for filename in all_files:
        try:
            # Open file
            df_temp = BurpFileReader(filename).to_pandas()
    
            df_temp = df_temp[['stnid','idtyp','lon','lat','elev','date','e_bufrid','rval','flgs']] # Keep only wanted columns
            df_temp = df_temp[df_temp['e_bufrid'].isin(meteo_variable.values())] # Drop lines where e_bufrid is not in list

            # Filtre sur lat/lon
            # On veut garder lat>10 et -178>lon>-13
            df_temp = df_temp.loc[(df_temp['lon'] >= -178.0+360.0) & (df_temp['lon'] <= -13.0+360.0) & (df_temp['lat'] > 10.0)]

            df_data = df_data.append(df_temp, ignore_index=True)
        except:
            continue

    df_data.to_pickle('/home/aul001/reanalyse/validation-v3/count-stations/data/burp/'+isd_or_ade+'/temp/data-'+year+'-'+month+'.pkl')

    return df_data

def count_data(year, month):

    isd_or_ade = 'hadisd'

    meteo_variable = {
      'temperature':12004.0,
      'dewpoint':   12006.0,
      'precip_01h': 13019.0,
      'precip_03h': 13020.0,
      'precip_06h': 13021.0,
      'precip_12h': 13022.0,
      'precip_24h': 13023.0#,
      #'snow_depth': 13013.0
    }
 
    input_path = '/home/aul001/reanalyse/validation-v3/count-stations/data/burp/'+isd_or_ade+'/temp/'

    colonnes = [x+' '+month for x in meteo_variable.keys() ] 

    df = pd.DataFrame(columns=['ID','idtyp','lon','lat']+colonnes)
    df_data = pd.read_pickle('/home/aul001/reanalyse/validation-v3/count-stations/data/burp/'+isd_or_ade+'/temp/data-'+year+'-'+month+'.pkl')

    # Station ID list
    id_list  = df_data['stnid']
    lon_list = df_data['lon']
    lat_list = df_data['lat']

    df['ID']  = id_list
    df['idtyp'] = df_data['idtyp']
    df['lon'] = lon_list
    df['lat'] = lat_list
    df = df.drop_duplicates(subset='ID')

    id_list = df['ID'].to_list() # Update id_list with dublons removed

    # Count number of records
    for ID in id_list:
        df_temp = df_data[df_data['stnid'] == ID]

        for variable_name in meteo_variable.keys():
            variable = meteo_variable[variable_name]
            df_temp2 = df_temp[df_temp['e_bufrid'] == variable]
            count    = (df_temp2['rval'].values != [-99999.0]).sum()

            df[variable_name+' '+month].loc[df['ID'] == ID] = count

    return df


if __name__ == '__main__':

    isd_or_ade = 'hadisd'

    meteo_variable = {
      'temperature':12004.0,
      'dewpoint':   12006.0,
      'precip_01h': 13019.0,
      'precip_03h': 13020.0,
      'precip_06h': 13021.0,
      'precip_12h': 13022.0,
      'precip_24h': 13023.0#,
      #'snow_depth': 13013.0
    }
 
    # Start pool of multiprocesses
    pool = Pool(12)

    #year_list = np.arange(2013, 2014, 1)
    #year_list = [str(y) for y in year_list]
    year_list = ['2013']
    
    month_list = ['01','02','03','04','05','06','07','08','09','10','11','12'] 

    for year in year_list:

        print(year)
    
        # Iteration on each month of the year
        df1 = pool.starmap(read_data, [(year,'01'), (year,'02'), (year,'03'), (year,'04'), (year,'05'), (year,'06'), (year,'07'), (year,'08'), (year,'09'), (year,'10'), (year,'11'), (year,'12')])

        df_all = pool.starmap(count_data, [(year,'01'), (year,'02'), (year,'03'), (year,'04'), (year,'05'), (year,'06'), (year,'07'), (year,'08'), (year,'09'), (year,'10'), (year,'11'), (year,'12')])

        for i in range(len(df_all)):
            if i == 0:
                df = df_all[i]
            else:
                df = pd.merge(df, df_all[i], on=['ID','idtyp','lon','lat'], how='outer')

        for variable_name in meteo_variable.keys():
            variable = meteo_variable[variable_name]
            column_list = [ variable_name+' '+m for m in month_list  ]
            rename_dict = dict(zip(column_list, month_list))

            df_var = df[['ID','idtyp','lon','lat'] + column_list]
            df_var = df_var.rename(columns=rename_dict)

            df_var.to_pickle('data/burp/'+isd_or_ade+'/'+year+'-'+str(int(variable))+'.pkl')

                
