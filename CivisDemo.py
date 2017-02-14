import civis
client = civis.APIClient()

import pandas as pd

my_table = civis.io.read_civis_sql(sql="select* from geocoding.housing_units limit 10",database="FEMA",use_pandas=True)

final_dataframe = pd.DataFrame(my_table, columns = ['housing_unit_id','state_code'])

civis.io.query_civis(sql='drop table if exists public.dewberry_training', database='FEMA')

civis.io.dataframe_to_civis(df=final_dataframe, database='FEMA', table='public.dewberry_training')
