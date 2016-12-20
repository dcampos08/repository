# Civis API calls with the Civis R Client

# Need to be anonymized!

library( civis.r.client )

# Example of reading in a table from Redshift
#tpc <- read_civis( 
#  database='Civis Database',
#  tablename='retail.customers'
#)

# Example of querying from Redshift
#tpc.example <- read_civis(
#  database='Civis Database',
#  sql=paste( 'select * from retail.customers limit 50' )
#)

# Example of writing to Readshift
#write_civis(
#  df=tpc.example,
#  tablename='retail.customers_limited',
#  database='Civis Database',
#  distkey='person_id',
#  sortkey1='person_id'
#)

