import psycopg2
from psycopg2 import sql, errors
from psycopg2.extras import RealDictCursor
import os
from datetime import datetime, timezone
import logging
import psycopg2.extras
from psycopg2 import extras,errors
import traceback

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

DDBB_INFO = {
    "user": os.getenv('POSTGRES_USER', ''),
    "password": os.getenv('POSTGRES_PASSWORD', ''),
    "host": os.getenv('POSTGRES_HOST', ''),
    "port": os.getenv('POSTGRES_PORT', ),
    "database": os.getenv('POSTGRES_DB', ''),
}

datatype_map = {
    "BOOL": "boolean",
    "INT": "integer",
    "UINT_BCD": "integer",
    "REAL": "float4",
    "DINT": "bigint",
    "UDINT": "bigint",
    "STRING": "varchar(255)",
    "STRING(20)": "varchar(255)",
    "CHAR": "char(1)",
    "WORD": "integer",
    "DWORD": "bigint"
}


def create_db_table(name, db_items):
    with psycopg2.connect(**DDBB_INFO) as con:
        with con.cursor() as cur:
            columns = []

            columns += [
                sql.SQL(f"{{name}} {datatype_map[item['type'].upper()]} NULL").format(name=sql.Identifier(item['variable'])) for
                item in db_items]
            columns.append(sql.SQL('{name} timestamptz NOT NULL').format(name=sql.Identifier('createdAt')))

            cur.execute(
                sql.SQL("""CREATE TABLE IF NOT EXISTS openiot.{table} 
                        ({fields})
                        """).format(
                    table=sql.Identifier(name),
                    fields=sql.SQL(', ').join(columns)
                ))


def insert_db_data(name, db_data, db_items=[]):
    query= F"""INSERT INTO elliot.metric_numeric_data (metric_id, ts, value) VALUES %s """
    insert_data=[]
    with psycopg2.connect(**DDBB_INFO) as con:
        with con.cursor() as cur:

            columns = list(db_data.keys())
            values = [db_data[key] if not isinstance(db_data[key], str) else db_data[key].replace("\x00", " ") for key in db_data]
            for i in range(len(columns)):
                insert_data.append([columns[i],str(datetime.now(timezone.utc)),values[i]])

            try:
                extras.execute_values(cur, query, insert_data)  # execute the whole data insert into the new table
                con.commit()

            except (Exception, psycopg2.Error) as error:
                raise Exception(f'- Error at insert new data: {error}')

            finally:
                if con:
                    logging.info("Data inserted into:", )


def read_db_desc(name_table, type='siemens'):
    items_db = []
    try:
        with psycopg2.connect(**DDBB_INFO) as connection:
            with connection.cursor() as cursor:
                if type == 'siemens':
                    query = f'''select "metric_id","offset","type"
                                from elliot.{name_table}
                                where "offset" is not null and
                                coalesce("type", '') != ''
                                order by "offset"'''
                elif type == 'omron':
                    query = f'''select "static", offset_omron, tipo_omron
                                    from openiot.{name_table}_desc
                                    where coalesce(offset_omron, '') != '' and
                                            coalesce(tipo_omron, '') != '' and 
                                            habilitado
                                    order by "offset"'''
                else:
                    return {}

                cursor.execute(query)
                items = cursor.fetchall()
                for item in items:
                    items_db.append({"metric_id": item[0], "offset": item[1], "type": item[2]})
                return items_db
    except (Exception, psycopg2.Error) as error:
        logging.exception(f"Error while connecting to PostgreSQL {error, Exception}")


def read_offset(name_db, name_var, type='siemens'):
    try:
        with psycopg2.connect(**DDBB_INFO) as connection:
            with connection.cursor() as cursor:
                if type == 'siemens':
                    query = f'''select "offset"
                                from openiot.{name_db}_desc
                                where "static" = \'{name_var}\''''
                elif type == 'omron':
                    query = f'''select offset_omron
                                from openiot.{name_db}_desc
                                where "static" = \'{name_var}\''''
                else:
                    return {}

                cursor.execute(query)
                item = cursor.fetchone()
        if item is None:
            return None
        return item[0]
    except (Exception, psycopg2.Error) as error:
        logging.exception(f"Error while connecting to PostgreSQL {error, Exception}")



