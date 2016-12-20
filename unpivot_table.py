import os
import civis
import pandas as pd

input_table = os.environ['scratch.states_years_pivot']
id_vars = os.environ['state']
new_column_name = os.environ['year']
output_table = os.environ['scratch.states_years_container']

df = civis.io.read_civis(table=input_table
                         ,database=326
                         ,use_pandas=True)

df_melt = pd.melt(df,id_vars=id_vars,var_name=new_column_name)

print(df_melt)

long_table_result = civis.io.dataframe_to_civis(df_melt,database=326
                                               ,table=output_table
                                               ,existing_table_rows='truncate')
