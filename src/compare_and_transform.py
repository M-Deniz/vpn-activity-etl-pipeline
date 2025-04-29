#!/usr/bin/env python

import argparse
import sys
import jaydebeapi # for Oracle JDBC connection
import phoenixdb # for Phoenix connection

def parse_args():
    parser = argparse.ArgumentParser(description="Compare Oracle and Phoenix data, transform, then write back to Phoenix.")
    parser.add_argument('--oracle-host', required=True, help='db1.preprod.internal')
    parser.add_argument('--oracle-port', required=True, help='1521')
    parser.add_argument('--oracle-service-name', required=True, help='ORCLPDB1')
    parser.add_argument('--oracle-user', required=True, help='etl_user')
    parser.add_argument('--oracle-password', required=True, help='Oracle password')
    parser.add_argument('--oracle-driver-path', required=True, help='/opt/jdbc/ojdbc8.jar')
    parser.add_argument('--phoenix-url', required=True, help='http://phoenix-queryserver.internal:8765/')
    parser.add_argument('--phoenix-table', required=True, help='analytics.vpn_user_deltas')
    parser.add_argument('--source-table', default='MYSCHEMA.MY_ORACLE_SOURCE_TABLE', help='VPN_USER_ACCESS')
    parser.add_argument('--replica-table', default='MYSCHEMA.MY_PHOENIX_REPLICA_TABLE', help='vpn_user_replica')
    parser.add_argument('--last-updated-col', default='LAST_UPDATED', help='MODIFIED_AT')
    parser.add_argument('--id-col', default='ID', help='ID')
    return parser.parse_args()

def create_oracle_jdbc_connection(host, port, service_name, user, password, driver_path):

    #create JDBC connection to Oracle using JayDeBeApi
    jdbc_url = f"jdbc:oracle:thin:@{host}:{port}/{service_name}"

    driver_class = "oracle.jdbc.OracleDriver"

    conn = jaydebeapi.connect(
        jclassname=driver_class,
        url=jdbc_url,
        driver_args=[user, password],
        jars=[driver_path]
    )
    return conn

def get_oracle_data_jdbc(host, port, service_name, user, password, driver_path, source_table, last_updated_col):

    # Connect to Oracle via JDBC
    conn = create_oracle_jdbc_connection(host, port, service_name, user, password, driver_path)
    cursor = conn.cursor()

    query = f"""
        SELECT
            ID,
            SESSION_ID,
            LOGIN_TIME,
            LOGOUT_TIME,
            IP_ADDRESS,
            USER_NAME,
            DEVICE_TYPE,
            {last_updated_col}
        FROM {source_table}
        -- Optionally a WHERE clause for incremental loads:
        -- WHERE {last_updated_col} >= :last_run_time
    """
    
    cursor.execute(query)
    
    # fetch all rows
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    # convert to list of dicts
    oracle_data = []
    for row in rows:
        row_dict = dict(zip(columns, row))
        oracle_data.append(row_dict)

    cursor.close()
    conn.close()
    return oracle_data


# connect to Phoenix (HBase) and retrieve rows from the replica table
def get_phoenix_data(phoenix_url, replica_table, last_updated_col):

    query = f"""
        SELECT 
            ID,
            SESSION_ID,
            LOGIN_TIME,
            LOGOUT_TIME,
            IP_ADDRESS,
            USER_NAME,
            DEVICE_TYPE,
            {last_updated_col}
        FROM {replica_table}
    """
    rows_data = []

    conn = phoenixdb.connect(phoenix_url, autocommit=True)
    cursor = conn.cursor()
    cursor.execute(query)

    for row in cursor:
        row_dict = {
            'ID': row[0],
            'SESSION_ID': row[1],
            'LOGIN_TIME': row[2],
            'LOGOUT_TIME': row[3],
            'IP_ADDRESS': row[4],
            'USER_NAME': row[5],
            'DEVICE_TYPE': row[6],
            'LAST_UPDATED': row[7]
        }
        rows_data.append(row_dict)

    return rows_data

# find new or updated records in Oracle compared to Phoenix data
def compare_data(oracle_data, phoenix_data, id_col='ID', last_updated_col='LAST_UPDATED'):

    # build a dict keyed by ID for quick lookup
    phoenix_dict = {row[id_col]: row for row in phoenix_data}
    new_or_updated = []

    for o_row in oracle_data:
        o_id = o_row[id_col]
        if o_id not in phoenix_dict:
            # new row
            new_or_updated.append(o_row)
        else:
            # compare relevant fields
            oracle_ts = o_row.get(last_updated_col)
            phoenix_ts = phoenix_dict[o_id].get(last_updated_col)
            # if last_updated is more recent or other fields differ => updated
            if oracle_ts > phoenix_ts or o_row['SESSION_ID'] != phoenix_dict[o_id]['SESSION_ID']:
                new_or_updated.append(o_row)
            if oracle_ts > phoenix_ts or o_row['LOGIN_TIME'] != phoenix_dict[o_id]['LOGIN_TIME']:
                            new_or_updated.append(o_row)
            if oracle_ts > phoenix_ts or o_row['LOGOUT_TIME'] != phoenix_dict[o_id]['LOGOUT_TIME']:
                            new_or_updated.append(o_row)
            if oracle_ts > phoenix_ts or o_row['IP_ADDRESS'] != phoenix_dict[o_id]['IP_ADDRESS']:
                            new_or_updated.append(o_row)
            if oracle_ts > phoenix_ts or o_row['USER_NAME'] != phoenix_dict[o_id]['USER_NAME']:
                            new_or_updated.append(o_row)
            if oracle_ts > phoenix_ts or o_row['DEVICE_TYPE'] != phoenix_dict[o_id]['DEVICE_TYPE']:
                            new_or_updated.append(o_row)

    return new_or_updated

def transform_data(rows):

    # transforms start here
    transformed = []
    for r in rows:
        new_row = r.copy()
        new_row['SESSION_ID'] = r['SESSION_ID'].upper() if r['SESSION_ID'] else None
        new_row['LOGIN_TIME'] = r['LOGIN_TIME'].upper() if r['LOGIN_TIME'] else None
        new_row['LOGOUT_TIME'] = r['LOGOUT_TIME'].title() if r['LOGOUT_TIME'] else None
        new_row['IP_ADDRESS'] = r['IP_ADDRESS'].title() if r['IP_ADDRESS'] else None
        new_row['USER_NAME'] = r['USER_NAME'].title() if r['USER_NAME'] else None
        new_row['DEVICE_TYPE'] = r['DEVICE_TYPE'].title() if r['DEVICE_TYPE'] else None
        new_row['TRANSFORM_NOTES'] = f"Transformed row with ID={r['ID']}"
        transformed.append(new_row)
    return transformed

def write_to_phoenix(phoenix_url, table_name, data):

    # write transformed data into Phoenix table
    if not data:
        print("No data to write to Phoenix. Skipping.")
        return

    conn = phoenixdb.connect(phoenix_url, autocommit=False)
    cursor = conn.cursor()

    upsert_query = f"""
        UPSERT INTO {table_name}
        (ID, SESSION_ID, LOGIN_TIME, LOGOUT_TIME, IP_ADDRESS, USER_NAME, DEVICE_TYPE, TRANSFORM_NOTES)
        VALUES (?, ?, TO_TIMESTAMP(?), TO_TIMESTAMP(?), ?, ?, ?, ?)
    """

    for row in data:
        cursor.execute(
            upsert_query,
            (
                row['ID'],
                row['SESSION_ID'],
                row['LOGIN_TIME'],
                row['LOGOUT_TIME'],
                row['IP_ADDRESS'],
                row['USER_NAME'],
                row['DEVICE_TYPE'],
                row.get('TRANSFORM_NOTES', '')
            )
        )
    conn.commit()
    conn.close()

    print(f"Wrote {len(data)} rows to Phoenix table: {table_name}")

def main():
    args = parse_args()

    # fetch data from Oracle
    oracle_data = get_oracle_data_jdbc(
        host=args.oracle_host,
        port=args.oracle_port,
        service_name=args.oracle_service_name,
        user=args.oracle_user,
        password=args.oracle_password,
        driver_path=args.oracle_driver_path,
        source_table=args.source_table,
        last_updated_col=args.last_updated_col
    )
    print(f"Retrieved {len(oracle_data)} records from Oracle table {args.source_table}.")

    # fetch data from Phoenix
    phoenix_data = get_phoenix_data(
        phoenix_url=args.phoenix_url,
        replica_table=args.replica_table,
        last_updated_col=args.last_updated_col
    )
    print(f"Retrieved {len(phoenix_data)} records from Phoenix table {args.replica_table}.")

    # compare data sets
    new_or_updated = compare_data(
        oracle_data,
        phoenix_data,
        id_col=args.id_col,
        last_updated_col=args.last_updated_col
    )
    print(f"Found {len(new_or_updated)} new/updated records in Oracle.")

    if not new_or_updated:
        print("No new or updated records to transform or write. Exiting.")
        sys.exit(0)

    # transform data
    transformed = transform_data(new_or_updated)
    print(f"Transformed {len(transformed)} records.")

    # write to Phoenix
    write_to_phoenix(args.phoenix_url, args.phoenix_table, transformed)

if __name__ == "__main__":
    main()
