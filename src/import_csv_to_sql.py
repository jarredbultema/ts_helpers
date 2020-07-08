import json, csv, os
from getpass import getpass, getuser
from sqlalchemy import create_engine, exc
import os
import pandas as pd

def main():

    # Get arguments from input
    file_name, tablename, database, schema, postgres_password = get_args()

    # Connect to the database
    engine = create_engine("postgres://" + getuser() + ':' + postgres_password + "@/" + database)

    # Load the csv into PostGres
    print(f"\nLoading {file_name} into {database}.{schema}.{tablename}")
    try:
        conn = engine.connect()
        csv_to_table(tablename, schema, file_name, conn)
    except FileNotFoundError:
        print("*******\nYou may want to try giving the full filepath to the file, instead of the relative path.\n*******")
    finally:
        conn.close()
        engine.dispose()

    print('\nDone!')

def get_args():
    default_database = # database name
    default_schema = # default schema

    file_name = input('What is the complete path and name of the csv file?\n\t')

    tablename = os.path.splitext(os.path.basename(file_name))[0]
    tablename = tablename.replace('-', '_').replace(' ', '_')
    new_tablename = input(f"Postgres tablename: (or hit Enter to use {tablename}\n\t")
    if new_tablename:
        tablename = new_tablename

    database = input(f"Postgres Database: (or hit Enter to use {default_database})\n\t")
    if not database:
        database = default_database

    schema = input(f"Schema: (or hit Enter to use {default_schema})\n\t")
    if not schema:
        schema = default_schema

    postgres_password = getpass('Postgres Password: ')

    return file_name, tablename, database, schema, postgres_password


def csv_to_table(tablename, schema, file_name, conn, recursed=False):
    # Delete prior table
    conn.execute(f'''
        DROP TABLE IF EXISTS {schema}.{tablename}
        ;
    ''')

    # create the table
    with open(file_name, 'r') as f:
        header = next(csv.reader(f))
    datatypes = ',\n'.join([f"{col} text" for col in header])
    conn.execute(f'''
        CREATE TABLE {schema}.{tablename} (
            {datatypes}
        );
    ''')

    # Load data into the table
    try:
        conn.execute(f'''
            COPY {schema}.{tablename}
            FROM '{file_name}'
            CSV DELIMITER ',' HEADER
            ;
        ''')
        conn.execute("commit")
    except exc.DataError as e:
        # use pandas to get rid of bad encoding issues
        new_filename = file_name + '_tmp'
        pd.read_csv(file_name, dtype=str, engine='python').to_csv(new_filename, index=False)

        conn.execute(f'''
            COPY {schema}.{tablename}
            FROM '{new_filename}'
            CSV DELIMITER ',' HEADER
            ;
        ''')
        conn.execute("commit")

    num_rows = list(conn.execute(f'SELECT count(*) FROM {schema}.{tablename};'))[0][0]
    print(f"\t{num_rows} rows loaded.")

    conn.close()


if __name__ == '__main__':
    main()
