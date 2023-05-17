# Databricks notebook source
from IPython.display import display
from datetime import datetime
import ipywidgets as widgets
from scenario_class import Scenario
from databricks.sdk.runtime import spark
import pandas as pd
import numpy as np

scenario = Scenario()

# COMMAND ----------

optimisation = scenario.optimisation.df
assumption = scenario.assumption.df.set_index('period_end').sort_index()

PC_VSC_flow = optimisation['PC_VSC_flow']
PC_solar_production = optimisation['PC_solar_production']
operational_capacity = assumption.loc[assumption['identifier'] == 'PC_battery_operational_capacity']

BESS_df = pd.DataFrame()
num_inverters = 1554

BESS_df['Demand/Inverter'] = PC_VSC_flow.div(num_inverters)
BESS_df['PV Production/Inverter'] = PC_solar_production.div(num_inverters)
BESS_df['BESS Size'] = 20

# bess_index = []
# oc_index = operational_capacity.index
# for index in BESS_df.index:
#     i = index.strftime("%Y-%m-%d")
#     if i in oc_index:
#         BESS_df['BESS Size'].loc[index] = operational_capacity['numeric_value'].loc[i]

BESS_df['BESS Size'] = BESS_df['BESS Size']
BESS_df['Net Flow of Energy'] = BESS_df['PV Production/Inverter'] - BESS_df['Demand/Inverter']

charge_conditions = [BESS_df['Net Flow of Energy'] >= 0, BESS_df['Net Flow of Energy'] < 0]
charge_choices = ['Charge', 'Discharge']
BESS_df['Charge/Discharge'] = np.select(charge_conditions, charge_choices)

bess_states = []
spill = []
bess_state = 20
bess_effiency = 0.95
inverter_effiency = 0.95
bess_power = 2
current_spill = 0
for row in BESS_df.iterrows():
    net_flow = row[1][3]
    if bess_state + net_flow >= 0 and bess_state + net_flow <= bess_size:
        if net_flow > bess_power:
            current_spill += net_flow - bess_power
            bess_state += bess_power*bess_effiency
        else:
            bess_state += net_flow*bess_effiency
        bess_states.append(bess_state)
    spill.append(current_spill)

BESS_df['BESS State of Charge'] = b_size
BESS_df['Spill'] = spill
BESS_df

# COMMAND ----------

BESS_df.tail(30)

# COMMAND ----------

BESS_df.plot(y='Spill', use_index=True)

# COMMAND ----------

solcast_query = "select * from sandbox.pv_data.solcast__18_201125_133_408077"
df_solcast = spark.sql(solcast_query).toPandas()
optimisation = scenario.optimisation.df

num_rows = len(optimisation.index)
df_solcast = df_solcast.iloc[:num_rows]
df_solcast = df_solcast.drop(columns=['PeriodEnd', 'PeriodStart'])
optimisation = optimisation.reset_index(drop=True)
df = optimisation.join(df_solcast, how='outer')
df
