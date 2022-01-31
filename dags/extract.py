import pandas as pd
def Extract(**kwargs):
    print("//////////////////////////// IamIn ////////////////////////////")
    noc_regions = pd.read_csv("~/..//mnt/c/dags/noc_regions.csv")
    print("Ready")
    print(type(noc_regions))
    noc_regions_json = noc_regions.to_json(orient='index')
    kwargs['ti'].xcom_push(key='noc_regions_key', value=noc_regions_json)

    athlete_events = pd.read_csv("~/..//mnt/c/dags/athlete_events.csv")
    athlete_events_json = athlete_events.to_json(orient='index')
    kwargs['ti'].xcom_push(key='athlete_events_key', value=athlete_events_json)

    medals = pd.read_csv("~/..//mnt/c/dags/Medals.csv")
    medals_json = medals.to_json(orient='index')
    kwargs['ti'].xcom_push(key = 'medals_key' , value = medals_json)
