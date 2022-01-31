import pandas as pd
import numpy as np

def host_country(col):
    if col == "Rio de Janeiro":
        return "Brazil"
    elif col == "London":
        return "UK"
    elif col == "Beijing":
        return  "China"
    elif col == "Athina":
        return  "Greece"
    elif col == "Sydney" or col == "Melbourne":
        return  "Australia"
    elif col == "Atlanta" or col == "Los Angeles" or col == "St. Louis" or col=="Salt Lake City" or col=="Lake Placid" or col=="Squaw Valley":
        return  "USA"
    elif col == "Barcelona":
        return  "Spain"
    elif col == "Seoul":
        return  "South Korea"
    elif col == "Moskva" or col=="Sochi":
        return  "Russia"
    elif col == "Montreal" or col=="Calgary" or col =="Vancouver":
        return  "Canada"
    elif col == "Munich" or col == "Berlin" or col=="Garmisch-Partenkirchen":
        return  "Germany"
    elif col == "Mexico City":
        return  "Mexico"
    elif col == "Tokyo" or col=="Nagano"or col=="Sapporo" :
        return  "Japan"
    elif col == "Roma" or col=="Torino" or col=="Cortina d'Ampezzo":
        return  "Italy"
    elif col == "Paris" or col=="Albertville" or col=="Grenoble" or col=="Chamonix":
        return  "France"
    elif col == "Helsinki":
        return  "Finland"
    elif col == "Amsterdam":
        return  "Netherlands"
    elif col == "Antwerpen":
        return  "Belgium"
    elif col == "Stockholm":
        return  "Sweden"
    elif col == "Lillehammer" or col=="Oslo":
        return  "Norway"
    elif col == "Innsbruck":
        return  "Austria"
    elif col == "Sarajevo":
        return  "Bosnia and Herzegovina"
    elif col == "Sankt Moritz":
        return  "Switzerland"
    else:
        return "Other"


def FeatureEngineering(**kwargs):
    # noc_regions_json = kwargs['ti'].xcom_pull(key = 'noc_regions_key' ,task_ids='extract')
    # noc_regions_df = pd.read_json(noc_regions_json, orient='index' ,dtype=False )

    athlete_events_json = kwargs['ti'].xcom_pull(key = 'athlete_events_key' ,task_ids='extract')
    athlete_events_df = pd.read_json(athlete_events_json, orient='index' ,dtype=False )

    df_2021_Olympics_json = kwargs['ti'].xcom_pull(key = 'df_2021_Olympics_json_key' ,task_ids='data_integration')
    df_2021_Olympics = pd.read_json(df_2021_Olympics_json, orient='index' ,dtype=False )

    # medals_json = kwargs['ti'].xcom_pull(key = 'medals_key' ,task_ids='extract')
    # medals_df = pd.read_json(medals_json, orient='index' ,dtype=False )


    #=========== Feature Engineering code here ==========
    # df_feature = athlete_events_df.copy()
    #Add BMI Column
    df_feature = athlete_events_df.copy()
    df_feature['BMI'] = df_feature['Weight'] / ((df_feature['Height']/100)**2)
    #df_feature = df_feature.query('Season == "Summer"') # Only interested in Summer Olympics sience tokyo 2021 was a summer Olympic
    df_feature['Host_Country'] = df_feature['City'].apply(host_country) # add column host country to the atheletes dataSet
    print("Iam")
    print(df_feature)
    df_feature = df_feature.groupby(['Year','NOC','Host_Country','Medal'])['Medal'].count().unstack().fillna(0).astype(int).reset_index()
    # df_feature['Is_Host'] = np.where(df_feature['Host_Country'] == df_feature['region'],1,0) # add column isHost to the atheletes dataSet
    df_feature['Total_Medals'] = df_feature['Bronze'] + df_feature['Silver'] + df_feature['Gold']
    #*************************************************************************************
    df_tocyo_feature=df_2021_Olympics
    df_tocyo_feature['Is_Host'] = np.where(df_tocyo_feature['NOC'] == 'JPN',1,0) # add column is host to tokyo dataSet
    df_tocyo_feature['Host_Country'] = 'Japan' # add column hostCountry to tokyo dataSet
    #************************************************************************************
    df_featureFull = df_feature.append(df_tocyo_feature) #combine the 2 datasets
    df_featureFull.reset_index(drop=True, inplace=True) #Final dataSet after adding the features

     #=========== Feature Engineering code here ==========

    df_featureFull_json = df_featureFull.to_json(orient='index')
    kwargs['ti'].xcom_push(key = 'df_featureFull_json_key' , value = df_featureFull_json)
    
    # athlete_events_engineered_json = athlete_events_df.to_json(orient='index')
    # kwargs['ti'].xcom_push(key = 'athlete_events_engineered_key' , value = athlete_events_engineered_json)

    # medals_engineered_json = medals_df.to_json(orient='index')
    # kwargs['ti'].xcom_push(key = 'medals_engineered_key' , value = medals_engineered_json)

