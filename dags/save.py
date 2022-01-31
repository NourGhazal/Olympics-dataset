import pandas as pd

def Save(**kwargs):
    # noc_regions_transformed_json = kwargs['ti'].xcom_pull(key = 'noc_regions_blablabla_key' ,task_ids='blblblb')
    # noc_regions_transformed = pd.read_json(noc_regions_transformed_json, orient='index' ,dtype=False )
    # noc_regions_transformed.to_csv("saved/noc_regions_transformed_saved.csv")
 
    athlete_events_cleaned_json = kwargs['ti'].xcom_pull(key = 'athlete_events_cleaned_key' ,task_ids='data_cleaning')
    athlete_events_cleaned = pd.read_json(athlete_events_cleaned_json, orient='index' ,dtype=False )
    athlete_events_cleaned.to_csv("~/..//mnt/c/dags/saved/athlete_events_cleaned_saved.csv")

    # medals_transformed_json = kwargs['ti'].xcom_pull(key = 'medals_blablabla_key' ,task_ids='blblblb')
    # medals_transformed = pd.read_json(medals_transformed_json, orient='index' ,dtype=False )
    # medals_transformed.to_csv("saved/medals_transformed_saved.csv")

    df_All_Olympics_json = kwargs['ti'].xcom_pull(key = 'df_All_Olympics_json_key' ,task_ids='data_integration')
    df_All_Olympics = pd.read_json(df_All_Olympics_json, orient='index' ,dtype=False )
    df_All_Olympics.to_csv("~/..//mnt/c/dags/saved/df_All_Olympics_saved.csv")

    df_featureFull_json = kwargs['ti'].xcom_pull(key = 'df_featureFull_json_key' ,task_ids='feature_engineering')
    df_featureFull = pd.read_json(df_featureFull_json, orient='index' ,dtype=False )
    df_featureFull.to_csv("~/..//mnt/c/dags/saved/df_featureFull_saved.csv")

