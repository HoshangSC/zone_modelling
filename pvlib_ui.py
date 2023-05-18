# Databricks notebook source
# MAGIC %pip install pvlib

# COMMAND ----------

from IPython.display import display
from datetime import datetime
import ipywidgets as widgets
import pvlib as pl
from pvlib.pvsystem import PVSystem
from pvlib.modelchain import ModelChain
from pvlib.location import Location


class pvlib_wrapper():
    def __init__(self):
        """
        This is a wrapper around the PVlib library classes ModelChain, PVSystem and Location. It is designed to provide a UI for inputing required parameters and then running a simulation. The output of the simulation

        Simulation Steps:

        1. Start with Solcast data

        2. Extract the relevant parameters from the Solcast data, arange them into a dataframe called "weather" with the column names defined here:

        "weather" dataframe. Take the solcast columns, rename them with the correct pvlib names, return the weather dataframe.
        https://pvlib-python.readthedocs.io/en/stable/reference/generated/pvlib.modelchain.ModelChain.prepare_inputs.html#pvlib.modelchain.ModelChain.prepare_inputs
        Solcast Name        PVLib Name
        -----------         -----------
        PeriodStart         PeriodStart
        AirTemp             temp_air
        Dhi                 dhi
        Dni                 dni
        Ghi                 ghi
        WindSpeed10m        wind_speed


        
        Azimuth
        Zenith

        3. 

        2. Continue extracting the relevant data from the Solcast dataframe. Next, we need
        If SAT, determine tracking angle. If MAV, used fixed angles. (function "get_tilt_azimuth")

        3. Calculate POA Irradiance

        """
        # this is where I plan to initialise the UI.
        # self.ui = init_ui()

        # HOSHANG ---   Build the gui (functions below) that is able to return the following params. Common params are common. SAT/MAV inputs depend on whether array_type is SAT or MAV
        #               There will need to be a load button that when pressed, returns the variables and runs the "load_all()" function.

        # Common Params - Generic
        self.met_data = None #this will be the solcast dataframe
        self.met_data_name = None #this should be the name of the met_data (solcast) file
        self.weather = None
        self.array_type = None # string: 'SAT' or 'MAV'
        self.modules_per_string = 1
        self.strings_per_inverter = 1
        self.racking_model = None # string: ‘open_rack’, ‘close_mount’ or ‘insulated_back’
        
        # Common Params - Module
        self.bifacial = True
        self.bifacial_factor = 0.85
        self.module_type = None # string "glass_polymer" or "glass_glass"

        # SAT Params
        self.sat_axis_tilt = 0
        self.sat_axis_azimuth = 0
        self.sat_max_angle = 60
        self.sat_backtrack = True
        self.sat_pitch = 5
        self.sat_height = 1.5
        self.sat_mod_length = 2.1

        # MAV Params
        self.mav_tilt = 10
        self.mav_azimuth = 90


        ####################
        # These are parameters that do not need gui inputs
        # self.

    def get_tilt_azimuth(self):
        """
        This function needs to return values for surface_tilt and surface_azimuth for every time step in the simulation data.
        """
        if self.array_type == "SAT":
            # load solar position and tracker orientation for use in pvsystem object
            sat_mount = pvsystem.SingleAxisTrackerMount(axis_tilt=self.sat_axis_tilt,  # flat array
                                                        axis_azimuth=self.sat_axis_azimuth,  # north-facing azimuth
                                                        max_angle=self.sat_max_angle,  # a common maximum rotation
                                                        backtrack=self.sat_backtrack,
                                                        gcr=self.sat_mod_length / self.sat_pitch)
            # created for use in pvfactors timeseries
            orientation = sat_mount.get_orientation(filter_df['Zenith'],filter_df['Azimuth'])
            self.surface_tilt = orientation['surface_tilt']
            self.surface_azimuth = orientation['surface_azimuth']

        else:
            self.surface_tilt = self.mav_tilt
            self.surface_azimuth = self.mav_azimuth

    # "location" and "system" are parameters inherited from ModelChain. I am initialising them with the custom classes.
    def load_all(self):
        self.get_tilt_azimyth()
        self.location = Custom_Location()
        self.system = Custom_System()
        self.mc = ModelChain(self.system,self.location)

    # Pass met_data
    def pass_solcast(self):
        pass
        


class Custom_System(PVSystem):
    """
    Overwrite a few of the pvlib.PVSystem class functions
    """
    def __init__(self):
        # self.surface_tilt = None
        # self.surface_azimuth = None
        # self.albedo
        # self.surface_type

        # self.module
        # self.module_type
        # self.module_parameters

        # self.temperature_model_parameters

        # self.modules_per_string
        # self.strings_per_inverter
        # self.racking_model
        return



    # def get_irradiance(self):
    #     '''
    #     Override get_irradiance method, use infinite_sheds.get_irradiance if modules are bifacial.

    #     Args:

    #     Returns:
    #         None
    #     '''
    #     return


# class Custom_Location(Location):
#     """
#     Overwrite a few of the pvlib.PVSystem class functions
#     """
#     def __init__(self):



# COMMAND ----------

pv_model = Custom_System()

# COMMAND ----------

def get_met_data_sources():
    """
    Returns a list of table names in the data repository schema "sandbox.met_data"
    """
    return spark.sql("SHOW TABLES in sandbox.met_data").rdd.map(lambda x: x.tableName).collect()

def get_modules():
    """
    Return a list of all modules in the online database
    """
    # retrieve CEC module parameters from the SAM libraries
    # with the "name" argument set to "CECMod"
    # List also seen here:
    # https://solarequipment.energy.ca.gov/Home/PVModuleList

    # the CEC modules are a pandas DataFrame oriented as columns, transpose to arrange
    # as indices
    # CECMODS.T.head()
    # https://pvsc-python-tutorials.github.io/PVSC48-Python-Tutorial/Tutorial%204%20-%20Model%20a%20Module%27s%20Performance.html
    CECMODS = pl.pvsystem.retrieve_sam(path="https://raw.githubusercontent.com/NREL/SAM/develop/deploy/libraries/CEC%20Modules.csv")
    module_list = []
    for col in CECMODS.columns:
        module_list.append(col)
    
    return module_list


def pvlib_ui():
    output = widgets.Output()

    def on_value_change(change):
        with output:
            output.clear_output()
            print(change.new)

    def on_load_clicked(_):
        with output:
            output.clear_output()
            print(met_data_widget.value)
                

    sources = get_met_data_sources()
    met_data_widget = widgets.Dropdown(options=sources, description='Met Data', value=None)
        
    
    module_widget = widgets.Dropdown(options=get_modules(), description='Module Name', value=None)

    array_type_widget = widgets.RadioButtons(options=['SAT', 'MAV'], description='Array Type', value=None)
    array_type_widget.observe(on_value_change, 'value')

    load_button_widget = widgets.Button(description="Load")
    load_button_widget.on_click(on_load_clicked)
    display(met_data_widget, module_widget, array_type_widget, load_button_widget, output)


# COMMAND ----------

pvlib_ui()

# COMMAND ----------


