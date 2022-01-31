import pandas as pd

def DataIntegration(**kwargs):
    noc_regions_json = kwargs['ti'].xcom_pull(key = 'noc_regions_key' ,task_ids='extract')
    df_noc_regions = pd.read_json(noc_regions_json, orient='index' ,dtype=False )

    athlete_events_json = kwargs['ti'].xcom_pull(key = 'athlete_events_cleaned_key' ,task_ids='data_cleaning')
    df_athelete_events = pd.read_json(athlete_events_json, orient='index' ,dtype=False )

    medals_json = kwargs['ti'].xcom_pull(key = 'medals_key' ,task_ids='extract')
    df_medals = pd.read_json(medals_json, orient='index' ,dtype=False )


    #=========== Data Integration code here ==========

    #merge the athelete DataSet with nocRegion DataSet
    df_merged = pd.merge(df_athelete_events, df_noc_regions, on='NOC', how='left')

    # Merge the medals DataSet with nocRegion DataSet to get the noc and region column instead of the team/noc column

    df_merged_medal_nocRegion = pd.merge(df_medals, df_noc_regions , left_on="Team/NOC" , right_on="region", how='left')
    df_merged_medal_nocRegion.drop_duplicates(subset=['Team/NOC'] , inplace=True ,ignore_index=True)
    df_merged_medal_nocRegion_NotNa = df_merged_medal_nocRegion[df_merged_medal_nocRegion.region.notna()]
    df_merged_medal_nocRegion_NAN = df_merged_medal_nocRegion[df_merged_medal_nocRegion.region.isna()]
    NOC_NaN_Manual_arr = ['USA','CHN','GBR','RUS','KOR' , 'IRI' , 'TPE' , 'HKG' , 'MKD' , 'CIV' , 'MDA' , 'SYR']
    df_merged_medal_nocRegion_NAN['NOC'] = NOC_NaN_Manual_arr
    df_NOC_NaN_Manual_handled = pd.merge(df_merged_medal_nocRegion_NAN.drop(columns=['region', 'notes'], axis=1) ,df_noc_regions,how='left')
    df_merged_medal_nocRegion_final = pd.concat([df_merged_medal_nocRegion_NotNa , df_NOC_NaN_Manual_handled] , ignore_index=True)


    #IMPORTANT
    regions = df_noc_regions.copy()
    df = df_athelete_events.copy()
    df_21 =df_medals.copy()
    df_21_full =df_medals.copy()
    df = df_merged.copy()

    # get the medals of the historic dataset from 1896-2016
    df_oldOlympics = df.groupby(['Year','Season','region','NOC','Medal'])['Medal'].count().unstack().fillna(0).astype(int).reset_index()
    df_oldOlympics['Total_Medals'] = df_oldOlympics['Bronze'] + df_oldOlympics['Silver'] + df_oldOlympics['Gold']
    
    # get the dataset for tokyo2021 after adding noc,region,year,season columns.
    df_2021_Olympics = df_merged_medal_nocRegion_final[['NOC', "Gold", "Silver", "Bronze","region"]]
    df_2021_Olympics['Total_Medals'] = df_2021_Olympics[["Gold", "Silver", "Bronze"]].sum(axis=1)
    df_2021_Olympics['Year'] = 2021
    df_2021_Olympics['Season'] = "Summer"

    #The new DataSet after the integration.
    # Adding 2021 data to historic   *****>>>> last output <<<<<*****
    df_All_Olympics = df_oldOlympics.append(df_2021_Olympics)
    df_All_Olympics.reset_index(drop=True, inplace=True)
    df_All_Olympics # Final DataSet after integrating
    
    #=========== Data Integration integrated code here ==========

    # noc_regions_integrated_json = noc_regions_df.to_json(orient='index')
    # kwargs['ti'].xcom_push(key = 'noc_regions_integrated_key' , value = noc_regions_integrated_json)
    
    # athlete_events_integrated_json = athlete_events_df.to_json(orient='index')
    # kwargs['ti'].xcom_push(key = 'athlete_events_integrated_key' , value = athlete_events_integrated_json)

    # medals_integrated_json = medals_df.to_json(orient='index')
    # kwargs['ti'].xcom_push(key = 'medals_integrated_key' , value = medals_integrated_json)

    df_2021_Olympics_json = df_2021_Olympics.to_json(orient='index')
    kwargs['ti'].xcom_push(key = 'df_2021_Olympics_json_key' , value = df_2021_Olympics_json)

    df_All_Olympics_json = df_All_Olympics.to_json(orient='index')
    kwargs['ti'].xcom_push(key = 'df_All_Olympics_json_key' , value = df_All_Olympics_json)

