from enum import Enum
from numbers import Number
import psycopg2
from psycopg2 import sql
import boto3
import os
import configparser
import argparse
import re


class PSQLDataType(Enum):
    STR = 'text'
    NUM = 'numeric'
    BOOL = 'bool'
    NULL = 'text'
    SET = 'text'
    DICT = 'JSON'
    LIST = 'text'


def to_pg_datatype(data):
    if isinstance(data, str):
        return PSQLDataType.STR.value
    elif isinstance(data, bool):
        return PSQLDataType.BOOL.value
    elif isinstance(data, Number):
        return PSQLDataType.NUM.value
    elif isinstance(data, set):
        return PSQLDataType.SET.value
    elif isinstance(data, list):
        return PSQLDataType.LIST.value
    elif data is None:
        return PSQLDataType.NULL.value
    else:
        return PSQLDataType.NULL.value


def is_table_empty(psql_db, table_name):
    conn = pg_conn(psql_db)
    schema = psql_db['schema']
    with conn:
        with conn.cursor() as curs:
            try:
                s = "SELECT to_regclass('{}.{}');".format(schema, table_name)
                curs.execute(s)
                exist = curs.fetchone()
                if not exist[0]:
                    return
                s = "SELECT * from {}.{} limit 1".format(schema, table_name)
                curs.execute(s)
                data = curs.fetchone()
            except psycopg2.Error as e:
                raise SystemExit(e)
            else:
                if data:
                    raise SystemExit("Table {}.{} is not empty. Please empty the table and run this script again.".format(schema, table_name))


def migrate(psql_db, dynamodb_table_name, data_dynamodb):
    print("Migrating from dynamodb to psql....")
    table_name = re.sub('[][}{\\/\-:*?\"<>|]', '_', dynamodb_table_name)
    table_name = table_name.lower()
    is_table_empty(psql_db, table_name)
    pk = 1
    seen_columns = list()
    schema_name = psql_db['schema']

    conn = pg_conn(psql_db)
    with conn:
        with conn.cursor() as curs:
            curs.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {0}.{1} (pk integer PRIMARY KEY);").format(sql.Identifier(schema_name), sql.Identifier(table_name)))
            for datum in data_dynamodb:
                counter = 0
                for k in datum:
                    col_data = datum[k]
                    col_pg_data_type = to_pg_datatype(col_data)

                    if isinstance(col_data, dict):
                        # ignoring nested data.
                        continue
                    elif isinstance(col_data, set) or isinstance(col_data, list):
                        col_data = ', '.join(col_data)

                    if k not in seen_columns:
                        seen_columns.append(k)
                        a = [schema_name, table_name, k, col_pg_data_type]
                        sq = sql.SQL("ALTER TABLE {0} ADD COLUMN IF NOT EXISTS {1} {2};").format(sql.SQL('. ').join([sql.Identifier(a[0]),
                                                                                                 sql.Identifier(a[1])]), sql.Identifier(a[2]), sql.Identifier(a[3]))
                        curs.execute(sq)

                    if counter == 0:
                        col = seen_columns[0]
                        b = [schema_name, table_name, col]

                        sq = sql.SQL("INSERT INTO {0}.{1} ({2}, {3}) VALUES (%s, %s)").format(sql.Identifier(b[0]), sql.Identifier(b[1]), sql.Identifier('pk'), sql.Identifier(b[2]))
                        curs.execute(sq, [pk, col_data])

                    else:
                        c = [schema_name, table_name, k]
                        sq = sql.SQL("UPDATE {0}.{1} SET {2} = %s WHERE pk = %s;").format(sql.Identifier(c[0]), sql.Identifier(c[1]), sql.Identifier(c[2]))
                        curs.execute(sq, [col_data, pk])

                    counter += 1
                pk += 1

    print("Finish.")


def pg_conn(psql_db):
    try:
        conn = psycopg2.connect("dbname={} user={} host={} password={}".format(
            psql_db['dbname'], psql_db['user'], psql_db['host'], psql_db['password']))
        return conn
    except Exception as e:
        raise SystemExit(e)


def read_aws_credentials(location, account):
    config = configparser.ConfigParser()
    path = os.path.expanduser(location)
    config_settings = config.read(path)
    if not config_settings:
        return None

    if 'aws_access_key_id' not in config[account] and 'aws_secret_access_key' not in config[account]:
        raise Exception("Couldn't find aws key in ".format(location))

    ACCESS_KEY = config[account]['aws_access_key_id']
    SECRET_ACCESS_KEY = config[account]['aws_secret_access_key']

    return ACCESS_KEY, SECRET_ACCESS_KEY


def aws_credentials():
    creds = read_aws_credentials('~/.boto', 'Credentials')
    if not creds:
        creds2 = read_aws_credentials('~/.aws/credentials', 'default')
        if not creds2:
            raise Exception("Couldn't find aws credentials files in ~/.boto and ~/.aws/credentials")

    return creds[0], creds[1]


def dynamodb_conn(region):
    ACCESS_KEY, SECRET_ACCESS_KEY = aws_credentials()

    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_ACCESS_KEY,
        region_name=region,
    )

    return dynamodb


def scan_dyanmodb(dynamodb, dynamodb_table_name):
    table = dynamodb.Table(
        dynamodb_table_name,
    )

    print('---------------------------------------------------')
    print('Scanning data in dynamodb...')
    response = table.scan()
    data_dynamodb = list()

    for i in response['Items']:
        data_dynamodb.append(i)

    while 'LastEvaluatedKey' in response:
        response = table.scan(
            ExclusiveStartKey=response['LastEvaluatedKey']
        )

        for i in response['Items']:
            data_dynamodb.append(i)

    print(data_dynamodb)
    print('Scan data is done.')
    print('---------------------------------------------------')
    return data_dynamodb


def main(args):
    dynamodb_table_name = args.dynamodb_table_name

    psql_db = {
        'dbname': args.dbname,
        'user': args.user,
        'host': args.host,
        'password': args.password,
        'port': args.port,
        'schema': args.schema_name
    }

    dynamodb = dynamodb_conn(args.region)
    data_dynamodb = scan_dyanmodb(dynamodb, dynamodb_table_name)
    migrate(psql_db, dynamodb_table_name, data_dynamodb)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='DynamoToPG', description='Migrate dynamodb to psql. Dict is omitted - ignoring nested data.')
    parser.add_argument('-r', dest='region', required=True)
    parser.add_argument('-dt', dest='dynamodb_table_name', required=True)
    parser.add_argument('-hs', dest='host', required=True)
    parser.add_argument('-p', dest='port', default='5432')
    parser.add_argument('-u', dest='user', required=True)
    parser.add_argument('-d', dest='dbname', required=True)
    parser.add_argument('-pass', dest='password', default='')
    parser.add_argument('-s', dest='schema_name', default='public')

    args = parser.parse_args()
    main(args)
