import civis

client = civis.APIClient()

my_tables = client.tables.list(database_id = 326, schema = 'mailing')

for tt in my_tables:
    print(tt['name'])
