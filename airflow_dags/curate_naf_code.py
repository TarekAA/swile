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
            return data
        else:
            print(f'Failed to retrieve data for siret={siret} ; reason: {response.status_code}')
            return None
        pass


@dataclass
class NAFAPIRequester(NAFAPIRequest):
    siret_list: list[str] = field(default=None)
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
        engine = get_postgres_engine()
        self.naf_code_df.to_sql(self.table_name, engine, if_exists='append', index=False)


def get_siret_to_be_mapped() -> list[str]:
    engine = get_postgres_engine()
    df = pd.read_sql('SELECT DISTINCT siret FROM siret_to_be_mapped', engine)
    siret_list = df['siret'].tolist()
    return siret_list


def generate_siret_to_be_mapped_table():
    cli_args = ["run", "--select", "siret_naf", "siret_to_be_mapped",
                "--project-dir", "swile/dbt/swile",
                "--profiles-dir", "swile/"]
    run_dbt_project(cli_args)


def write_naf_codes():
    siret_list = get_siret_to_be_mapped()
    naf_requester = NAFAPIRequester(token=INSEE_API_TOKEN, siret_list=siret_list,
                                    table_name='siret_naf_staging')
    naf_requester.write_to_db()


if __name__ == "__main__":
    # NAFAPIRequest('ee03bc97-73cf-3756-8c55-e6c2bedcebd9').get_naf_code('41622001001280')
    generate_siret_to_be_mapped_table()
    write_naf_codes()
