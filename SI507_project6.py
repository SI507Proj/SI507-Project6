# Import statements
import psycopg2
from psycopg2 import sql
import psycopg2.extras
from config import *
import csv
import re
import sys


# Write code / functions to set up database connection and cursor here.
def get_connection_and_cursor():
    print(db_name)
    try:
        if db_password != "":
            db_connection = psycopg2.connect("dbname='{0}' user='{1}' password='{2}'".format(db_name, db_user, db_password))
            print("Success connecting to database")
        else:
            db_connection = psycopg2.connect("dbname='{0}' user='{1}'".format(db_name, db_user))
    except:
        print("Unable to connect to the database. Check server and credentials.")
        sys.exit(1) # Stop running program if there's no db connection.

    db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    return db_connection, db_cursor

# Write code / functions to create tables with the columns you want and all database setup here.
def setup_database(conn, cur):
    # Invovles DDL commands
    # DDL --> Data Definition Language
    # CREATE, DROP, ALTER, RENAME, TRUNCATE

    #cur.execute("""DROP TABLE IF EXISTS "States" CASCADE""")
    #cur.execute("""DROP TABLE IF EXISTS "Sites" """)

    # Create States table
    cur.execute("""CREATE TABLE IF NOT EXISTS "States" (
                "ID" SERIAL PRIMARY KEY,
                "Name" VARCHAR(40) UNIQUE
                )""")
    conn.commit()

    # Create Sites table
    cur.execute("""CREATE TABLE IF NOT EXISTS "Sites" (
                "ID" SERIAL,
                "Name" VARCHAR(128) UNIQUE,
                "Type" VARCHAR(128),
                "State_ID" INTEGER,
                FOREIGN KEY ("State_ID") REFERENCES "States" ("ID"),
                "Location" VARCHAR(255),
                "Description" TEXT
                )""")
    conn.commit()

    print('Setup database complete')

# Write code / functions to deal with CSV files and insert data into the database here.
# Make sure to commit your database changes with .commit() on the database connection.
def insert(conn, cur, table, data_dict):
    column_names = data_dict.keys()
    # print(column_names)

    # generate insert into query string
    query = sql.SQL("""INSERT INTO "{0}"({1}) VALUES({2}) ON CONFLICT DO NOTHING RETURNING "ID" """).format(
        sql.SQL(table),
        sql.SQL(', ').join(map(sql.Identifier, column_names)),
        sql.SQL(', ').join(map(sql.Placeholder, column_names))
    )
    query_string = query.as_string(conn)
    cur.execute(query_string, data_dict)

def select(cur, table, param, state):
    cur.execute("""SELECT "{}" FROM "{}" WHERE "Name"='{}' """.format(param, table, state))
    result = cur.fetchone()
    if result is not None:
        return result[param]
    return None

def save_csv_to_db(conn, cur, files):
    setup_database(conn, cur)
    for file_str in files:
        with open(file_str, 'r') as file:
            states_dict = {}
            csv_reader = csv.reader(file)
            next(csv_reader)
            m = re.match(r"(\w+).csv", file_str)
            state_name = m.group(1)[0].upper() + m.group(1)[1:]
            states_dict["Name"] = state_name
            insert(conn, cur, "States", states_dict)
            state_id = select(cur, "States", "ID", state_name)
            for row in csv_reader:
                sites_dict = {}
                sites_dict["Name"] = row[0]
                sites_dict["Type"] = row[2]
                sites_dict["Location"] = row[1]
                sites_dict["Description"] = row[4]
                sites_dict["State_ID"] = state_id
                insert(conn, cur, "Sites", sites_dict)

            conn.commit()


# Write code to make queries and save data in variables here.
"""
In Python, query the database for all of the locations of the sites. (Of course, this data may vary
 from "Detroit, Michigan" to "Various States: AL, AK, AR, OH, CA, NV, MD" or the like. That's OK!)
 Save the resulting data in a variable called all_locations.

In Python, query the database for all of the names of the sites whose descriptions include the word beautiful.
 Save the resulting data in a variable called beautiful_sites.

In Python, query the database for the total number of sites whose type is National Lakeshore.
 Save the resulting data in a variable called natl_lakeshores.

In Python, query your database for the names of all the national sites in Michigan.
 Save the resulting data in a variable called michigan_names. You should use an inner join query to do this.

In Python, query your database for the total number of sites in Arkansas.
 Save the resulting data in a variable called total_number_arkansas. You can use multiple queries + Python code to do this,
 one subquery, or one inner join query. HINT: You'll need to use an aggregate function!
"""

def do_queries(conn, cur):

    cur.execute("""SELECT "Location" FROM "Sites" """)
    result = cur.fetchall()
    all_locations = []
    for pair in result:
        all_locations.append(pair["Location"])
    print(all_locations)

    cur.execute("""SELECT "Name" FROM "Sites" WHERE "Description" like '%beautiful%'""")
    # cur.execute("SELECT * FROM \"Sites\" WHERE \"Description\" like '%beautiful%'")
    result = cur.fetchall()
    # print(result)
    beautiful_sites = []
    for pair in result:
        beautiful_sites.append(pair["Name"])
    print(beautiful_sites)

    cur.execute("""SELECT * FROM "Sites" WHERE "Type"='National Lakeshore'""")
    result = cur.fetchall()
    natl_lakeshores = len(result)
    print(natl_lakeshores)

    cur.execute("""SELECT "Sites"."Name" FROM "Sites" INNER JOIN "States" on ("Sites"."State_ID"=
                "States"."ID") WHERE "States"."Name"='Michigan'""")
    result = cur.fetchall()
    michigan_names = []
    for pair in result:
        michigan_names.append(pair["Name"])
    print(michigan_names)

    cur.execute("""SELECT COUNT(*) FROM "Sites" INNER JOIN "States" on ("Sites"."State_ID"=
                "States"."ID") WHERE "States"."Name"='Arkansas'""")
    result = cur.fetchall()
    total_number_arkansas = result[0]['count']
    print(total_number_arkansas)


# Write code to be invoked here (e.g. invoking any functions you wrote above)

if __name__ == '__main__':
    conn, cur = get_connection_and_cursor()
    files = ["arkansas.csv", "california.csv", "michigan.csv"]
    save_csv_to_db(conn, cur, files)
    do_queries(conn, cur)

# We have not provided any tests, but you could write your own in this file or another file, if you want.
