import os

import civis

client=civis.APIClient()

events = civis.io.read_civis_sql(sql="SELECT e.eventid, max(es.eventshiftid) as eventshiftid, max(l.locationid) as locationid FROM vansync.tsm_etgs_events_mym e JOIN vansync.tsm_etgs_eventshifts_mym es USING (eventid) JOIN vansync.tsm_etgs_eventslocations_mym l USING (eventid) GROUP BY 1 ORDER BY 1",database="Everytown for Gun Safety",use_pandas=True)

drop = civis.io.query_civis(sql="drop table if exists public.python_example", database='Everytown for Gun Safety')

drop.result()

civis.io.dataframe_to_civis(df=events, database='Everytown for Gun Safety', table='public.python_example')
