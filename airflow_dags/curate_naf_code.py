"""scrit that contain logic to request INSEE API and write siret <-> naf_code mapping
    back into staging table, then moving data into production table.
    Runs DBT commands
"""

import requests
from dataclasses import dataclass, field
import psycopg2
import pandas as pd
from swile.airflow_dags.constants import *
from swile.airflow_dags.load_event_data import run_dbt_project
import typing


def get_postgres_db_cursor() -> typing.Tuple["_T_conn", "cursor"]:
    """Get postgres connection with info from constants.py

    :return: conn database connection, cur database cursor
    """
    pg_config = dict(**PG_CONFIG)
    pg_config['dbname'] = DB_NAME
    conn = psycopg2.connect(**pg_config)
    cur = conn.cursor()
    return conn, cur


@dataclass
class NAFAPIRequest:
    """Class responsible to sending a single INSEE API request.

    """
    token: str
    url: str = ("https://api.insee.fr/entreprises/sirene/V3/siret/{siret}"
                "?champs=activitePrincipaleEtablissement%2CactivitePrincipaleUniteLegale")

    def get_naf_code(self, siret: str):
        """Get naf_code from siret code by calling INSEE API

        :param siret: str, 14 digit code
        :return: str, naf_code
        """
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {self.token}'
        }
        params = {
            'champs': 'activitePrincipaleEtablissement,activitePrincipaleUniteLegale'
        }
        url_formatter = self.url.format(siret=siret)
        response = requests.get(url_formatter, headers=headers, params=params)

        if response.status_code == 200:
            # Parse the JSON response
            data = response.json()
            naf_code = data.get('etablissement').get('uniteLegale').get('activitePrincipaleUniteLegale')
            # naf_code = (data.get('etablissement').get('periodesEtablissement')[0]
            #             .get('activitePrincipaleEtablissement'))
            return naf_code
        else:
            print(f'Failed to retrieve data for siret={siret} ; reason: {response.status_code}')
            return None
        pass


@dataclass
class NAFAPIRequester(NAFAPIRequest):
    """ Class responsible for sending multiple API request for each available siret value.
    """

    siret_list: list[str] = field(default=None)
    schema: str = field(default='general')
    table_name: str = field(default='siret_naf_staging')
    naf_code_df: pd.DataFrame = field(init=False)

    def get_all_naf_codes(self):
        """Send INSEE API calls for each available siret value in siret_list.
        Results are written to the dataframe naf_code_df

        :return: None
        """
        siret_naf_mapping_list = []
        for siret in self.siret_list:
            naf_code = self.get_naf_code(siret)
            siret_naf_mapping_list.append((siret, naf_code))
        naf_code_df = pd.DataFrame(siret_naf_mapping_list, columns=['siret', 'naf_code'])
        self.naf_code_df = naf_code_df

    def write_to_db(self):
        """Insert obtained siret <-> naf_code mapping values into postgres db

        :return: None
        """
        conn, cur = get_postgres_db_cursor()
        columns = ', '.join(self.naf_code_df.columns)
        sql_template = f"INSERT INTO {self.schema}.{self.table_name} ({columns}) VALUES "

        values_list = []
        for index, row in self.naf_code_df.iterrows():
            items_ = [f"'{item}'" if isinstance(item, str) else str(item) for item in row]
            values = ', '.join(items_)
            values_list.append(f"({values})")
        sql_statement = sql_template + ', '.join(values_list) + ";"

        cur.execute(sql_statement)
        conn.commit()
        cur.close()
        conn.close()


def get_siret_to_be_mapped() -> list[str]:
    """Obtain siret values that we don't have their naf_code mapping yet.
    We'll send API calls to only those with missing naf_code values in datbase

    :return: siret_list, list of siret values to be mapped to naf_code
    """
    conn, cur = get_postgres_db_cursor()
    cur.execute('SELECT DISTINCT siret FROM general.siret_to_be_mapped')
    res = cur.fetchall()
    siret_list = [val[0] for val in res]
    return siret_list


def generate_siret_to_be_mapped_table():
    """Run the DBT model that contains siret values with no previously available naf_code mapping

    :return: None
    """
    # we run siret_naf here to make sure the table get created when it's run for the first time
    # we will recall back at the end
    cli_args = ["run", "--select", "siret_naf", "siret_to_be_mapped",
                "--project-dir", f"{DIR_PATH}dbt/swile",
                "--profiles-dir", f"{DIR_PATH}"]
    run_dbt_project(cli_args)


def generate_siret_naf_table():
    """Run the DBT model that contains siret <-> naf_code mapping. This is the production table.

    :return: None
    """
    cli_args = ["run", "--select", "siret_naf",
                "--project-dir", f"{DIR_PATH}dbt/swile",
                "--profiles-dir", f"{DIR_PATH}"]
    run_dbt_project(cli_args)


def generate_spent_table(dir_path_or_relative=''):
    """Run the DBT model that final report of spent by date and naf_code.

    :return: None
    """

    cli_args = ["run", "--select", "spent",
                "--project-dir", f"{DIR_PATH}dbt/swile",
                "--profiles-dir", f"{DIR_PATH}"]
    run_dbt_project(cli_args)


def write_naf_codes():
    """Get siret values that will be mapped, Map them to naf_codes
    and finally write them into the staging table siret_naf_staging

    :return: None
    """
    siret_list = get_siret_to_be_mapped()
    naf_requester = NAFAPIRequester(token=INSEE_API_TOKEN,
                                    siret_list=siret_list,
                                    schema='general',
                                    table_name='siret_naf_staging')
    naf_requester.get_all_naf_codes()
    naf_requester.write_to_db()


if __name__ == "__main__":
    generate_siret_to_be_mapped_table()
    write_naf_codes()
    generate_siret_naf_table()
    generate_spent_table()
