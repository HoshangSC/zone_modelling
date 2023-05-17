import pandas as pd
from IPython.display import display, clear_output
from datetime import datetime
import ipywidgets as widgets
from databricks.sdk.runtime import spark
import pytz
from pyspark.sql.functions import *
from pyspark.sql.types import *

class Scenario:
    """
    A class to represent a given Scenario.
    ...
    Attributes
    ----------
    name : str
        name of the scenario
    start : str
        start time of the scenario
    end : str
        end time of the scenario
    optmisation : object
        optimisation table given from scenario, start and end time
    assumption : object
        assumption table given from scenario, start and end time
    Methods
    -------
    None
    """
    def __init__(self, name=None, start=None, end=None):
        '''
        Initialises Scenario
        Args:
            name (str) : name of the scenario
            start (str) : start time of the scenario
            end (str) : end time of the scenario
        Returns:
            None
        '''
        self.name = name
        self.start = start
        self.end = end
        self.optimisation = None
        self.assumption = None
        self.scopti_databricks_ui(True)
        self.timezone = "Australia/Darwin"
    
    def print_scenario_details(self):
        print(f"Scenario Name   : {self.name}")
        print(f"Start Date/Time : {self.start}")
        print(f"End Date/Time   : {self.end}")
        print(f"Time Zone       : {self.timezone}")
    
    def load(self):
        '''
        Load the select scenario optimisation and assumptions.
        Args:
            None
        Returns:
            None
        '''
        print("Scenario Loading, this may take several seconds...")
        self.optimisation = Optimisation(self.name, self.start, self.end, self.timezone)
        self.assumption = Assumption(self.name, self.start, self.end)
        clear_output()
        print("Scenario Loaded")
        self.print_scenario_details()

    def filterdf(self):
        '''
        Filter the scenario DF based on selected parameters.
        Args:
            None
        Returns:
            None
        '''

    def get_all_scenarios(self):
        '''
        Gets all scenarios from the details table
        Args:
            None
        Returns:
                df_scenarios (pandas dataframe object): Dataframe that gives result of all scenarios
        '''
        scenario_details_query = "select * from hive_metastore.federated_postgres.federated_optimisations_scenario_details"
        df_scenarios = spark.sql(scenario_details_query).toPandas().sort_values('scenario')
        return df_scenarios

    def get_active_scenarios(self):
        '''
        Gets the active scenarios from the details table
        Args:
            None
            
        Returns:
                df_scenarios (pandas dataframe object): Dataframe that gives result of active scenarios
        '''
        scenario_details_query = "select * from hive_metastore.federated_postgres.federated_optimisations_scenario_details where is_active == true"
        df_scenarios = spark.sql(scenario_details_query).toPandas().sort_values('scenario')
        return df_scenarios
    
    def scopti_databricks_ui(self, is_active):
        '''
        Creates Databricks UI widgets for a Databricks notebook.
        Args:
            is_active (boolean) : Tells UI whether or not to select active scenarios
        Returns:
                None: This function displays the created widgets.
        '''

        # List of hours in a day in form '00:00:00'
        hours = ['00:00:00', '01:00:00', '02:00:00', '03:00:00', '04:00:00', '05:00:00', '06:00:00', '07:00:00', '08:00:00', '09:00:00', '10:00:00', '11:00:00', 
        '12:00:00', '13:00:00', '14:00:00', '15:00:00', '16:00:00', '17:00:00', '18:00:00', '19:00:00', '20:00:00', '21:00:00', '22:00:00', '23:00:00', '24:00:00']

        # List of selectable timezones
        timezone_list = ["Australia/Darwin", "UTC", "Singapore"]

        # Create widgets for the timezone selection
        timezones = widgets.RadioButtons(options=timezone_list, value=timezone_list[0], description='Timezone:',disabled=False)

        # Creates widgets for start and end time parameters
        start_hour = widgets.SelectionSlider(options=hours,value=hours[0], description='Start Hour',
        disabled=False, continuous_update=False, orientation='horizontal', readout=True)
        start_date = widgets.DatePicker(description='Start Date', disabled=False)

        end_hour = widgets.SelectionSlider(options=hours,value=hours[0], description='End Hour',
        disabled=False, continuous_update=False, orientation='horizontal', readout=True)
        end_date = widgets.DatePicker(description='End Date', disabled=False)

        # Creates Scenario dropdown depending on active scenarios are required
        if is_active == True:
            scenarios = self.get_active_scenarios()
        elif is_active == False:
            scenarios = self.get_all_scenarios()
        scenario_widget = widgets.Dropdown(options=scenarios['scenario'], description='Scenario')  

        # Create button widget. Clicking this buttons loads scenario from queried table.
        load_button = widgets.Button(description="Load")

        # Output widget to display the loaded dataframes
        output = widgets.Output()

        def on_load_button_clicked(_):
            '''
            Handles load button, updates scenario object properties
            Args:
                None
            Returns:
                None
            '''
            with output:
                output.clear_output()
                self.name = scenario_widget.value
                if start_date.value is not None:
                    self.start = start_date.value.strftime("%Y-%m-%d") + ' ' + start_hour.value
                if end_date.value is not None:
                    self.end = end_date.value.strftime("%Y-%m-%d") + ' ' + end_hour.value
                self.timezone = timezones.value
                self.load()


        # Register the button's callback function to query df and display results to the output widget
        load_button.on_click(on_load_button_clicked)

        # Define Layout
        section_layout = widgets.Layout(
        border='solid 1px',
        margin='0px 10px 10px 0px',
        padding='5px 5px 5px 5px')

        # Collect widgets in boxes
        scenario_box = widgets.VBox([scenario_widget,load_button])

        start_box = widgets.HBox([start_date, start_hour])
        end_box = widgets.HBox([end_date, end_hour])
        filter_box = widgets.VBox([start_box,end_box])

        full_box = widgets.HBox([scenario_box, filter_box, timezones])
        full_box.layout = section_layout

        # Display the widgets and output
        display(full_box, output)

class Optimisation:
    """
    A class to represent a given scenarios' optimisation table.
    ...
    Attributes
    ----------
    name : str
        name of the optimisation scenario
    start : str
        start time of the queried optimisation table
    end : str
        end time of the queried optimisation table
    Methods
    -------
    create_optimisation_df():
        Creates an optimisation table for the scenario
    """
    def __init__(self, scenario, start, end, timezone):
        '''
        Initialises Optimisation Class
        Args:
            name (str) : name of the optimised scenario
            start (str) : start time of the optimised scenario
            end (str) : end time of the optimised scenario
        Returns:
            None
        '''
        self.scenario = scenario
        self.start = start
        self.end = end
        self.timezone = timezone
        self.df = self.create_optimisation_df()

    def create_optimisation_df(self):
        '''
        Creates Pandas Dataframe for optimsation table
        Args:
            None
        Returns:
            df_optimsations (pandas dataframe object) : Dataframe that gives optimsation of scenario
        '''
        if self.timezone == "UTC":
            tz = "period_end_utc"
        if self.timezone == "Singapore":
            tz = "period_end_sg"
        if self.timezone == "Australia/Darwin":
            tz = "period_end_nt"
        optimisations_query = "select "+tz+", identifier, value from hive_metastore.federated_postgres.federated_timezoned_optimisations where scenario = '" + self.scenario + "'"
        df_optimisations = spark.sql(optimisations_query)
        # pandas doesnt have a "decimal" data type, this needs to be cast as "double" prior to conversion
        df_optimisations = df_optimisations.withColumn('value', col('value').cast(DoubleType()))
        df_optimisations = df_optimisations.toPandas()
        df_optimisations = df_optimisations.set_index(pd.DatetimeIndex(df_optimisations[tz]))
        df_optimisations = df_optimisations.pivot(index=tz, columns='identifier', values='value')
        if self.start != None or self.end != None:
            df_optimisations = df_optimisations[self.start:self.end]    
        return df_optimisations

class Assumption:
    """
    A class to represent a given scenarios' assumption table.
    ...
    Attributes
    ----------
    name : str
        name of the scenario
    start : str
        start time of the queried assumption table
    end : str
        end time of the queried assumption table
    Methods
    -------
    create_assumption_df():
        Creates an assumption table for the scenario
    """
    def __init__(self, scenario, start, end):
        '''
        Initialises Assumption Class
        Args:
            name (str) : name of the assumption scenario
            start (str) : start time of the assumption scenario
            end (str) : end time of the assumption scenario
        Returns:
            None
        '''
        self.scenario = scenario
        self.start = start
        self.end = end
        self.df = self.create_assumption_df()

    def create_assumption_df(self):
        '''
        Creates Pandas Dataframe for assumption table
        Args:
            None
        Returns:
            df_assumptions (pandas dataframe object) : Dataframe that gives assumptions of scenario
        '''
        scenario_assumptions_query = "select * from hive_metastore.federated_postgres.federated_optimisations_assumptions where scenario = '" + self.scenario + "' order by period_start asc"
        df_assumptions = spark.sql(scenario_assumptions_query).toPandas()
        df_assumptions['period_start']= pd.to_datetime(df_assumptions['period_start'])
        df_assumptions['period_end']= pd.to_datetime(df_assumptions['period_end'])
        if self.start != None or self.end != None:
            df_assumptions = df_assumptions.loc[(df_assumptions['period_start'] >= self.start) & (df_assumptions['period_end'] <= self.end)]  
        return df_assumptions

