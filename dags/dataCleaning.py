# import matplotlib.pyplot as plt
# import seaborn as sns
import pandas as pd
import numpy as np
from scipy import stats
# from matplotlib.ticker import PercentFormatter

def DataCleaning(**kwargs):
    print('IamIn')
    # noc_regions_json = kwargs['ti'].xcom_pull(key = 'noc_regions_key' ,task_ids='extract')
    # df_noc_regions = pd.read_json(noc_regions_json, orient='index' ,dtype=False )

    athlete_events_json = kwargs['ti'].xcom_pull(key = 'athlete_events_key' ,task_ids='extract')
    # print(type(athlete_events_json))
    df_athelete_events = pd.read_json(athlete_events_json, orient='index',typ='frame',dtype=False )
    print(df_athelete_events)
    # df_athelete_events = pd.to_csv(df_athelete_events)
    print('IamIn2')

    # medals_json = kwargs['ti'].xcom_pull(key = 'medals_key' ,task_ids='extract')
    # medals_df = pd.read_json(medals_json, orient='index' ,dtype=False )


    #=========== Data cleaning code here ==========
    #mean imputting
    print("=============================== drop null ==============================")
    df_athelete_events["Age"].fillna(value=round(df_athelete_events["Age"].mean()), inplace=True)
    df_athelete_events["Height"].fillna(value=round(df_athelete_events["Height"].mean()), inplace=True)
    df_athelete_events["Weight"].fillna(value=round(df_athelete_events["Weight"].mean()), inplace=True)
    df_athelete_events["Medal"].fillna("NoMedal", inplace=True)
    
    print("/////////////////// Finished ////////////////////////")

    print("=============================== outliers age ==============================")
    Q1 = df_athelete_events['Age'].quantile(0.25)
    Q3 = df_athelete_events['Age'].quantile(0.75)
    IQR = Q3 - Q1
    print(IQR)
    cut_off = IQR * 1.5
    lower = Q1 - cut_off
    upper =  Q3 + cut_off
    print(lower,upper)
    df1 = df_athelete_events[df_athelete_events['Age']> upper]
    df2 = df_athelete_events[df_athelete_events['Age'] < lower]
    print('Total number of outliers are', df1.shape[0]+ df2.shape[0])
    df_After_Age = df_athelete_events[(df_athelete_events['Age'] < upper) & (df_athelete_events['Age'] > lower)]


    print("=============================== outliers Height ==============================")
    Q1 = df_After_Age['Height'].quantile(0.25)
    Q3 = df_After_Age['Height'].quantile(0.75)
    IQR = Q3 - Q1
    print(IQR)
    cut_off = IQR * 1.5
    lower = Q1 - cut_off
    upper =  Q3 + cut_off
    print(lower,upper)
    df1 = df_After_Age[df_After_Age['Height']> upper]
    df2 = df_After_Age[df_After_Age['Height'] < lower]
    print('Total number of outliers are', df1.shape[0]+ df2.shape[0])
    df_After_Height = df_After_Age[(df_After_Age['Height'] < upper) & (df_After_Age['Height'] > lower)]


    print("=============================== outliers year ==============================")
    Q1 = df_After_Height['Year'].quantile(0.25)
    Q3 = df_After_Height['Year'].quantile(0.75)
    IQR = Q3 - Q1
    print(IQR)
    cut_off = IQR * 1.5
    lower = Q1 - cut_off
    upper =  Q3 + cut_off
    print(lower,upper)
    df1 = df_After_Height[df_After_Height['Year']> upper]
    df2 = df_After_Height[df_After_Height['Year'] < lower]
    print('Total number of outliers are', df1.shape[0]+ df2.shape[0])
    df_After_Year = df_After_Height[(df_After_Height['Year'] < upper) & (df_After_Height['Year'] > lower)]


    print("=============================== z score age ==============================")
    z_Age = np.abs(stats.zscore(df_After_Year['Age']))
    filtered_entries = z_Age < 2.5
    df_zscore_filter_A_zAge = df_After_Year[filtered_entries]

    print("=============================== z score Height ==============================")
    z_Height = np.abs(stats.zscore(df_zscore_filter_A_zAge['Height']))
    filtered_entries = z_Height < 2.4
    df_zscore_filter_A_zHeight = df_zscore_filter_A_zAge[filtered_entries]

    print("=============================== z score Weight ==============================")
    z_Weight = np.abs(stats.zscore(df_zscore_filter_A_zHeight['Weight']))
    filtered_entries = z_Weight < 3
    df_zscore_filter_A_zWeight = df_zscore_filter_A_zHeight[filtered_entries]


     #=========== Data cleaning code here ==========

    # noc_regions_cleaned_json = df_noc_regions.to_json(orient='index')
    # kwargs['ti'].xcom_push(key = 'noc_regions_cleaned_key' , value = noc_regions_cleaned_json)
    
    athlete_events_cleaned_json = df_zscore_filter_A_zWeight.to_json(orient='index')
    kwargs['ti'].xcom_push(key = 'athlete_events_cleaned_key' , value = athlete_events_cleaned_json)

    # medals_cleaned_json = medals_df.to_json(orient='index')
    # kwargs['ti'].xcom_push(key = 'medals_cleaned_key' , value = medals_cleaned_json)

