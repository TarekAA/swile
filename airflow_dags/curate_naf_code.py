import requests
from dataclasses import dataclass, field
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from swile.airflow_dags.constants import *
from swile.airflow_dags.load_event_data import run_dbt_project


def get_postgres_engine():
    pg_config = dict(**PG_CONFIG)
    pg_config['dbname'] = DB_NAME
    postgres_engine_url = "postgresql://{user}:{password}@{host}:5432/{dbname}"
    postgres_engine_url = postgres_engine_url.format(**pg_config)
    engine = create_engine(postgres_engine_url)
    return engine


def get_postgres_db_cursor():
    pg_config = dict(**PG_CONFIG)
    pg_config['dbname'] = DB_NAME
    conn = psycopg2.connect(**pg_config)
    cur = conn.cursor()
    return conn, cur


@dataclass
class NAFAPIRequest:
    token: str
    url: str = ("https://api.insee.fr/entreprises/sirene/V3/siret/{siret}"
                "?champs=activitePrincipaleEtablissement%2CactivitePrincipaleUniteLegale")

    def get_naf_code(self, siret: str):
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
    siret_list: list[str] = field(default=None)
    schema: str = field(default='general')
    table_name: str = field(default='siret_naf_staging')
    naf_code_df: pd.DataFrame = field(init=False)

    def get_all_naf_codes(self):
        siret_naf_mapping_list = []
        for siret in self.siret_list:
            naf_code = self.get_naf_code(siret)
            siret_naf_mapping_list.append((siret, naf_code))
        naf_code_df = pd.DataFrame(siret_naf_mapping_list, columns=['siret', 'naf_code'])
        self.naf_code_df = naf_code_df

    def write_to_db(self):
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
    conn, cur = get_postgres_db_cursor()
    cur.execute('SELECT DISTINCT siret FROM general.siret_to_be_mapped')
    res = cur.fetchall()
    siret_list = [val[0] for val in res]
    return siret_list


def generate_siret_to_be_mapped_table():
    # we run siret_naf here to make sure the table get created when it's run for the first time
    # we will recall back at the end
    cli_args = ["run", "--select", "siret_naf", "siret_to_be_mapped",
                "--project-dir", "swile/dbt/swile",
                "--profiles-dir", "swile/"]
    run_dbt_project(cli_args)


def generate_siret_naf_table():
    cli_args = ["run", "--select", "siret_naf",
                "--project-dir", "swile/dbt/swile",
                "--profiles-dir", "swile/"]
    run_dbt_project(cli_args)


def generate_spent_table():
    cli_args = ["run", "--select", "spent",
                "--project-dir", "swile/dbt/swile",
                "--profiles-dir", "swile/"]
    run_dbt_project(cli_args)


def write_naf_codes():
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
