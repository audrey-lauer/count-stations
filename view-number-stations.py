import numpy as np
import pandas as pd
import streamlit as st
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.backends.backend_agg import RendererAgg
from matplotlib.cm import get_cmap
import matplotlib.colors as mcolors
from streamlit_folium import folium_static, st_folium
import folium
from branca.colormap import linear, LinearColormap
from datetime import datetime, timedelta

import matplotlib.pylab as pylab
params = {'legend.fontsize': 'medium',
         'axes.labelsize':'large',
         #'axes.titlesize':'large',
         'xtick.labelsize': 'large',
         'ytick.labelsize': 'large'}
pylab.rcParams.update(params)

matplotlib.use("agg")
_lock = RendererAgg.lock

# Compability between pandas versions and mpl
pd.plotting.register_matplotlib_converters()

# Wide streamlit page
st.set_page_config(layout="wide")

#############
# Functions #
#############
@st.cache(hash_funcs={folium.folium.Map: lambda _: None}, allow_output_mutation=True)
def make_map(df_map_data, variable, field_to_color_by):
    main_map = folium.Map(location=(49, -105), zoom_start=3)

    # Maximum value
    vmax_dict = {
      'temperature':365*24,
      'dewpoint':   365*24,
      'precip_01h': 365*24,
      'precip_03h': 365*8,
      'precip_06h': 365*4,
      'precip_12h': 365*2,
      'precip_24h': 365,
      'snow_dept':  354*24
    }
    #colormap = linear.RdYlBu_11.scale(0,10000)
    colormap = LinearColormap([(12,22,61),(12,22,180),(12,22,220),'gray',(230,100,100),(230,40,40),(230,13,13)], vmin=0, vmax=vmax_dict[variable])
    colormap.caption = 'Yearly bias'
    colormap.add_to(main_map)

    for i in df_map_data.index:
        lat  = df_map_data['lat'].loc[i]
        lon  = df_map_data['lon'].loc[i]
        name = df_map_data['ID'].loc[i]

        value = df_map_data[field_to_color_by].loc[i]

        if np.isnan( value ):
            continue
        else:
            icon_color = colormap(value)

            folium.CircleMarker(location=[lat, lon],
                        fill=True,
                        fill_color=icon_color,
                        color=icon_color,
                        weight=1,
                        fill_opacity=0.8,
                        radius=3,
                        popup=name+'\n'+'Nb. obs: '+str(int(value)),
                        ).add_to(main_map)

    return main_map

def read_map_data(year, variable, data_type):

    month_list = ['01','02','03','04','05','06','07','08','09','10','11','12']
    
    if data_type == 'ISD':
        df = pd.read_pickle('data/spread/'+str(year)+'-'+variable+'.pkl')
        #df = pd.read_pickle('data/burp/isd/'+str(year)+'-'+variable+'.pkl')
    elif data_type == 'ISD-old':
        df = pd.read_pickle('data/burp/isd-old/'+str(year)+'-'+variable+'.pkl')
        df['lon'] = df['lon'] - 360.
    elif data_type == 'ADE':
        df = pd.read_pickle('data/burp/ade/'+str(year)+'-'+variable+'.pkl')
        df['lon'] = df['lon'] - 360.
    elif data_type == 'FROM GERARD':
        df = pd.read_pickle('data/burp/from-gerard/'+str(year)+'-'+variable+'.pkl')
        df['lon'] = df['lon'] - 360.
    elif data_type == 'HadISD':
        df = pd.read_pickle('data/burp/hadisd/'+str(year)+'-'+variable+'.pkl')
        df['lon'] = df['lon'] - 360.

    df['year'] = df[month_list].sum(axis=1)

    if data_type == 'ISD' or data_type == 'ISD-old' or data_type == 'HadISD':
        df = df[df['year'] > 0]
    elif data_type == 'ADE':
        df = df[df['year'] > 0]
        df = df[df['idtyp'] != 13]
        df = df[df['idtyp'] != 147]
    elif data_type == 'FROM GERARD':
        df = df[df['year'] > 0]

    # Crop to North America only
    df = df[(df['lat'] < 90)  & (df['lat'] > 10)  ]
    df = df[(df['lon'] < -13+360.) & (df['lon'] > -178+360.)]
    
    return df

def plot_timeserie(year_start, year_end, variable, data_type):
    year_list = np.arange(year_start,year_end,1)
    year_list = [str(y) for y in year_list]
    
    date_all  = []
    value_all = []
    df = pd.DataFrame()
    for year in year_list:
        filename = year+'-number-of-stations.csv'
    
        try:
            if data_type == 'ISD':
                df_temp = pd.read_csv('data/number-of-stations/isd/'+filename)
            elif data_type == 'ISD-old':
                df_temp = pd.read_csv('data/number-of-stations/isd-old/'+filename)
            elif data_type == 'ADE':
                df_temp = pd.read_csv('data/number-of-stations/ade/'+filename)
            elif data_type == 'FROM GERARD':
                df_temp = pd.read_csv('data/number-of-stations/from-gerard/'+filename)
            elif data_type == 'HadISD':
                df_temp = pd.read_csv('data/number-of-stations/hadisd/'+filename)
             
            df = df.append(df_temp, ignore_index=True)
        except:
            continue

    fig, ax = plt.subplots(1,figsize=(15,5))
    ax.plot(pd.to_datetime(df['date']), df[variable])
    plt.title('Number of obs for: '+variable)

    ax.xaxis.set_major_locator(matplotlib.dates.YearLocator(base=1))
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%Y"))
    plt.xticks(rotation=90);

    ax.grid(True)

    return fig

# Variable dictionary
meteo_variable = {
  'temperature':'12004',
  'dewpoint':   '12006',
  'precip_01h': '13019',
  'precip_03h': '13020',
  'precip_06h': '13021',
  'precip_12h': '13022',
  'precip_24h': '13023'#,
  #'snow_dept': '13013'
}


##########
# Format #
##########
st.title('Visualization of number of observations')

data_type = st.selectbox('Observation dataset',['ISD', 'ISD-old','ADE','FROM GERARD', 'HadISD'])

col1, col2 = st.columns([0.5,0.5])

with col1:
    st.header("Timeserie")

    if data_type == 'ISD':
        year_start, year_end = st.slider("Year range of timeserie",
                                         min_value=1950,
                                         max_value=2018,
                                         value=(1950, 2000))
    elif data_type == 'ISD-old':
        year_start, year_end = st.slider("Year range of timeserie",
                                         min_value=1950,
                                         max_value=2000,
                                         value=(1980, 2000))
    elif data_type == 'ADE':
        year_start, year_end = st.slider("Year range of timeserie",
                                         min_value=1994,
                                         max_value=2018,
                                         value=(1994, 2000))
    elif data_type == 'FROM GERARD':
        year_start, year_end = st.slider("Year range of timeserie",
                                         min_value=1953,
                                         max_value=2002,
                                         value=(1953, 2002))

    elif data_type == 'HadISD':
        year_start, year_end = st.slider("Year range of timeserie",
                                         min_value=2012,
                                         max_value=2016,
                                         value=(2013, 2014))

    variable_list = meteo_variable.keys()
    variable = st.selectbox('Variable',variable_list)

    fig = plot_timeserie(year_start, year_end, variable, data_type)

    st.write(fig)

# Interactive map
with col2:
    st.header("Interactive map")

    #year_map = st.number_input('Year to show on map', 1950,1999)
    #year_map = st.selectbox('Year to show on plot', ['all years'] + list( np.arange(year_start, year_end,1) ) )
    year_map = st.select_slider('Year to show on plot', np.arange(year_start, year_end,1))

    df_map_data = read_map_data(year_map, meteo_variable[variable], data_type)

    main_map = make_map(df_map_data, variable, 'year') 
    st_data = st_folium(main_map, width=850, height=700)




