library(civis.r.client)

tpc <- read_civis( 
  database='Civis Database',
  tablename='retail.customers'
)

tpc.example <- read_civis(
  database='Civis Database',
  sql=paste( 'select * from retail.customers limit 50' )
)

write_civis(
  df=tpc.example,
  tablename='retail.customers_limited',
  database='Civis Database',
  distkey='person_id',
  sortkey1='person_id'
)
