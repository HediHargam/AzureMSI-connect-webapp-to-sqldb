import os
import requests
import pyodbc
import pandas as pd
import struct
from logzero import logger


def get_data_from_sql_db(
    server: str, database: str, resource_uri: str, driver: str, query: str
) -> pd.DataFrame:
    """"Get data from SQL database with MSI Authentication and return df from executed query"""

    identity_endpoint = os.environ["IDENTITY_ENDPOINT"]
    identity_header = os.environ["IDENTITY_HEADER"]

    token_auth_uri = (
        f"{identity_endpoint}?resource={resource_uri}&api-version=2019-08-01"
    )
    head_msi = {"X-IDENTITY-HEADER": identity_header}

    resp = requests.get(token_auth_uri, headers=head_msi)
    access_token = bytes(resp.json()["access_token"], "utf-8")
    exptoken = b""
    for i in access_token:
        exptoken += bytes({i})
        exptoken += bytes(1)
    struct_token = struct.pack("=i", len(exptoken)) + exptoken

    with pyodbc.connect(
        "Driver=" + driver + ";Server=" + server + ";PORT=1433;Database=" + database,
        attrs_before={1256: bytearray(struct_token)},
    ) as conn:

        logger.info("Successful connection to database")

        with conn.cursor() as cursor:

            cursor.execute(query).fetchall()
            data = pd.read_sql(query, conn)

    return data


if __name__ == "__main__":

    df = get_data_from_sql_db(
        server="yourserver.database.windows.net",
        database="yourdatabasename",
        resource_uri="https://database.windows.net",
        driver="{ODBC Driver 17 for SQL Server}",
        query="SELECT * FROM YOURTABLE",
    )

    print(df.head())
