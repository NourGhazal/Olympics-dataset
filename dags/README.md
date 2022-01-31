# File Structure

This milestone contains 6 main python files :
    1.extract file.py
    2.data cleaning file.py
    3.data integration file.py
    4.feature engineering file.py
    5.save file.py
    6.dag file.py

And another 3 csv files contains our data :
    1.athlete_events.csv
    2.noc_regions.csv
    3.Medals.csv

And a "save" folder contains csv results after transformations.

## 1.extract file.py
    This file contains one function called "Extract" which executes in 3 steps:

    1.read csv file using pandas.read_csv(filename) as a pandas DataFrame
    2.convert this DataFrame into Json using .to_json function to make it serializable
    3.push this json file using xcom_push using a unique key

## 2.data cleaning file.py
    This file contains one function called "DataCleaning" which executes in 4 steps:

    1.pull the json file we pushed in Extract function using xcom_pull using the unique key
    after that we read this json as pandas DataFrame using .read_json function
    now we can use this DataFrame to clean our data.

    2.Clean Data by dropping null values.

    3.Clean Data by handling outliers.

    4.convert the cleaned DataFrame into Json using .to_json again
        and push this json file using xcom_push using a unique key to use this DataFrame in later transformation.


## 3.data integration file.py
    This file contains one function called "DataIntegration" which executes in 3 steps:

    1.pull the json files we pushed in DataCleaning function and Extract function using xcom_pull using the unique key after that we read this json as pandas DataFrame using .read_json function now we can use this DataFrame to integrate our data.

    2.Integrate Data by merging the 3 data sets.

    3.convert the integrated DataFrame into Json using .to_json again
    and push this json file using xcom_push using a unique key to use this DataFrame in later transformation.

## 4.feature engineering file.py
    This file contains one function called "FeatureEngineering" which executes in 3 steps:

    1.pull the json files we pushed in DataCleaning function and Extract function using xcom_pull using the unique key after that we read this json as pandas DataFrame using .read_json function now we can use this DataFrame to integrate our data.

    2.Engineer Data by adding two columns BMI column and Is_Host column.

    3.convert the Engineered DataFrame into Json using .to_json again
    and push this json file using xcom_push using a unique key to save this DataFrame.

## 5.save file.py
    This file contains one function called "Save" which executes in 3 steps:

    1.pull all json files from different transformation stages.

    2.read these json files as pandas DataFrames.

    3.save DataFrames as .csv files in saved folder.

## 6.dag file.py
    This file is the main file that pipelines our workflow and this file consists in 5 steps:

    1.Import Modules and Methods:
    Here we import all modules we need also we import the 5 methods from the 5 .py files.

    2.Default Arguments:
    Before creating our Dag object we have the the choice to explicitly pass a set of arguments to each task’s constructor, we can define a dictionary of default parameters that we can use when creating tasks.

    3.Instantiate a DAG:
    Here we create a DAG object to nest our tasks into. We pass a string that defines the dag_id, which serves as a unique identifier for your DAG. We also pass the default argument dictionary that we just defined and define a schedule_interval to run daily.

    4.Tasks:
    Tasks are generated when instantiating operator objects.In this milestone we used only python operator.
    The first argument task_id acts as a unique identifier for the task.
    The second argument python_callable takes the python function we need for this the task.
    The third argument dags takes the dag object we created in step3.

    5.Setting up Dependencies:
    We used the bit shift operator to define the flow of our task,
    for example t1 >> t2 this means that t2 will depend on t1

