# -*- coding: utf-8 -*-
"""
Created on Tue Mar 19 14:47:05 2024

@author: Diego Galdino, ODOT.
"""

''' NOTES
This script is based on Sam Granato (ODOT) procedures for TransCAD.
This script was designed to process NPMRDS datasets, which are very heavy CSV files.
To make this smoother, save memory as much as possible by:
    - using pd.to_numeric (https://pandas.pydata.org/docs/reference/api/pandas.to_numeric.html) 
    to downcast numerical columns to smallest numerical dtype possible (e.g., from 'float64' to 'float32')
    - using 'category' dtype instead of 'object'/'str'
    - not creating unnecessary columns as CONCAT and CONCAT2
    - avoiding .apply and row-wise operations. instead, use vectorized operations.
    - using 'inplace="True"' when available (pd.reset_index, pd.rename, pd.drop, pd.sort_values) instead of assigning copies of the dataframes.
    
Just changing the dtypes reduced the memory usage of the original dataset by 60%.
for more on optimizing pandas dataframes:
    https://vincentteyssier.medium.com/optimizing-the-size-of-a-pandas-dataframe-for-low-memory-environment-5f07db3d72e
'''

#%% Import Libraries

import pandas as pd
import numpy as np
import time
import os

#%% Set inputs

filename_ohint = './Datafiles/OHINT23/OHINT23.csv'
filename_ohintrk = './Datafiles/OHINTRK23/OHINTRK23.csv'
filename_ohsro = './Datafiles/OHSRO23/OHSRO23.csv'
filename_ohusr = './Datafiles/OHUSR23/OHUSR23.csv'

#%% Functions

def csv_preprocessing_pipeline(filename: str):
    # read csv
    data = pd.read_csv(filename, engine='pyarrow')
    # rename columns
    data.rename(columns={'measurement_tstamp':'DATETIME', 'speed':'MPH', 'travel_time_seconds':'TT_SEC'}, inplace=True)
    # optimize dtypes
    data['DATETIME'] = pd.to_datetime(data['DATETIME'])
    data['tmc_code'] = data['tmc_code'].astype('category')
    data['MPH'] = pd.to_numeric(data['MPH'], downcast='float')
    data['TT_SEC'] = pd.to_numeric(data['TT_SEC'], downcast='float')
    return data

def add_hod_dow_tod_to_dataset(data: pd.DataFrame):
    data = data.copy()
    ## create HOD (Hour Of Day)
    data['HOD'] = data['DATETIME'].dt.hour
    data['HOD'] = pd.to_numeric(data['HOD'], downcast='unsigned')
    ## create DOW (Day Of Week) and change them from 0-6 (Monday-Sunday) to 1-7 (Sunday-Saturday)
    data['DOW'] = (data['DATETIME'].dt.dayofweek + 2).replace(8,1)
    data['DOW'] = pd.to_numeric(data['DOW'], downcast='unsigned')
    ## create TOD (Time Of Day)
    data['TOD'] = 'EV'
    data.loc[(data['HOD'].between(5,10,inclusive='neither')) & (data['DOW'].between(2,6)), 'TOD'] = 'AM'
    data.loc[(data['HOD'].between(9,16,inclusive='neither')) & (data['DOW'].between(2,6)), 'TOD'] = 'MD'
    data.loc[(data['HOD'].between(15,20,inclusive='neither')) & (data['DOW'].between(2,6)), 'TOD'] = 'PM'
    data.loc[(data['HOD'].between(5,20,inclusive='neither')) & (data['DOW'].isin([1,7])), 'TOD'] = 'WE'
    data['TOD'] = data['TOD'].astype('category')
    return data

def add_lottr_to_dataset(data: pd.DataFrame, q1: int, q2: int):
    if (q1 < 0 or q1 > 100) or (q2 < 0 or q2 > 100):
        raise Exception('q1 and q2 must be between 0 and 100.')

    data = data.copy()
    data_perc = data.dropna(subset=['TT_SEC']).copy()
    ## create q1 and q2 travel time percentiles and merge them back to dataset
    data_perc = data_perc.groupby(['tmc_code','TOD']).agg(TTQ1P=('TT_SEC',lambda x: np.percentile(x, q=q1, method='closest_observation')),
                                                          TTQ2P=('TT_SEC',lambda x: np.percentile(x, q=q2, method='closest_observation'))).reset_index()
    data = pd.merge(data, data_perc, how='left')
    ## calculate LOTTR (Level Of Travel Time Reliability)
    data['LOTTR'] = pd.to_numeric(data['TTQ2P']/data['TTQ1P'], downcast='float')
    data.rename(columns={'TTQ1P':f'TT{q1}P', 'TTQ2P':f'TT{q2}P'}, inplace='True')
    return data

#%% Main code

# 1. Process interstate for all vehicles
t0 = time.time()
## run csv_prepocessing_pipeline on dataset
data_int = csv_preprocessing_pipeline(filename_ohint)
## run add_hod_dow_tod_to_dataset on dataset
data_int = add_hod_dow_tod_to_dataset(data_int)
## run add_lottr_to_dataset on dataset
data_int = add_lottr_to_dataset(data_int, q1=50, q2=80)
t1 = time.time()
print(f'total time for interstates (all vehicles): {t1-t0:.2f} seconds.')

# 2. Process interstate for trucks only
t0 = time.time()
## run csv_prepocessing_pipeline on dataset
data_int_trk = csv_preprocessing_pipeline(filename_ohintrk)
## merge with tmc_code and DATETIME from all vehicles just to create rows that are missed for trucks
data_int_trk = pd.merge(data_int[['tmc_code','DATETIME']].copy(), data_int_trk, how='left')
## run add_hod_dow_tod_to_dataset on dataset
data_int_trk = add_hod_dow_tod_to_dataset(data_int_trk)
## run add_lottr_to_dataset on dataset
data_int_trk = add_lottr_to_dataset(data_int_trk, q1=50, q2=95)
## rename trk columns
data_int_trk.columns = [f'TRK_{c}' for c in data_int_trk.columns]
t1 = time.time()
print(f'total time for interstates (trucks only): {t1-t0:.2f} seconds.')

# 3. Merge interstate trk into interstate all vehicles
t0 = time.time()
data_int_merged = pd.merge(data_int, data_int_trk, how='left', left_on=['tmc_code','DATETIME'], right_on=['TRK_tmc_code','TRK_DATETIME'])
## create FLAG and copy travel time from all vehicles to trucks
data_int_merged.loc[data_int_merged['TRK_TT_SEC'].isna(), 'FLAG'] = 1
data_int_merged['FLAG'] = data_int_merged['FLAG'].fillna(0)
data_int_merged['FLAG'] = pd.to_numeric(data_int_merged['FLAG'], downcast='unsigned')
## added the .to_numpy() at the end of the following line according to this: https://github.com/pandas-dev/pandas/issues/16187#issuecomment-657158866
data_int_merged.loc[data_int_merged['TRK_TT_SEC'].isna(), ['TRK_MPH','TRK_TT_SEC']] = data_int_merged.loc[data_int_merged['TRK_TT_SEC'].isna(), ['MPH','TT_SEC']].to_numpy()
data_int_merged.drop(columns=['TRK_tmc_code', 'TRK_DATETIME', 'TRK_HOD', 'TRK_DOW', 'TRK_TOD'], inplace=True)
## save file
filename_without_format = os.path.split(filename_ohint)[1].split('.')[0]
data_int_merged.to_csv(f'./LOTTR_{filename_without_format}.csv', index=False)
t1 = time.time()
print(f'total time for interstates (all vehicles + trucks only): {t1-t0:.2f} seconds.')

# 4. Process state routes for all vehicles
t0 = time.time()
## run csv_prepocessing_pipeline on dataset
data_sro = csv_preprocessing_pipeline(filename_ohsro)
## run add_hod_dow_tod_to_dataset on dataset
data_sro = add_hod_dow_tod_to_dataset(data_sro)
## run add_lottr_to_dataset on dataset
data_sro = add_lottr_to_dataset(data_sro, q1=50, q2=80)
## save file
filename_without_format = os.path.split(filename_ohsro)[1].split('.')[0]
data_sro.to_csv(f'./LOTTR_{filename_without_format}.csv', index=False)
t1 = time.time()
print(f'total time for state routes (all vehicles): {t1-t0:.2f} seconds.')

# 5. Process US routes for all vehicles
t0 = time.time()
## run csv_prepocessing_pipeline on dataset
data_usr = csv_preprocessing_pipeline(filename_ohusr)
## run add_hod_dow_tod_to_dataset on dataset
data_usr = add_hod_dow_tod_to_dataset(data_usr)
## run add_lottr_to_dataset on dataset
data_usr = add_lottr_to_dataset(data_usr, q1=50, q2=80)
## save file
filename_without_format = os.path.split(filename_ohusr)[1].split('.')[0]
data_usr.to_csv(f'./LOTTR_{filename_without_format}.csv', index=False)
t1 = time.time()
print(f'total time for US routes (all vehicles): {t1-t0:.2f} seconds.')