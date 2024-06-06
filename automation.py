# Import libraries required for connecting to mysql
import mysql.connector
# Import libraries required for connecting to PostgreSql
import psycopg2
# Connect to MySQL
connection = mysql.connector.connect(
	user='root', 
	password='enter your password',
	host='localhost',
	database='sales'
	)

# create cursor

mycursor = connection.cursor()

# Connect to PostgreSql
conn = psycopg2.connect(
   database= "sales", 
   user= 'postgres',
   password= 'Enter your password',
   host= 'localhost', 
   port= "5432"
)
cursor = conn.cursor()

# Find out the last rowid from PostgreSql data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on the  PostgreSql.

def get_last_rowid():
	cursor.execute('SELECT MAX(rowid) FROM sales_data;')
	row = cursor.fetchone()
	conn.commit()
	last_row = int(row[0])
	return last_row

last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

def get_latest_records(rowid):
	latest_rows= []
	mycursor.execute('SELECT * FROM sales_data WHERE rowid > %s;', (rowid,))
	for row in mycursor.fetchall():
		latest_rows.append(row)
	return latest_rows


new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))

# Insert the additional records from MySQL PostgreSql data warehouse.
def insert_records(records):	
	for record in records:
		statement= "INSERT INTO sales_data (rowid, product_id, customer_id, quantity) VALUES (%s,%s,%s,%s);"
		cursor.execute(statement, record)
		conn.commit()

insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# disconnect from mysql warehouse
connection.close()
# disconnect from PostgreSql data warehouse 
conn.close()
