import requests
import pandas as pd
import os
from datetime import datetime
import io

API = "https://healthdata.gov/resource/g62h-syeh.csv"
CHUNK_LIMIT = 1000000

DISEASE_COLS = {"influenza":
                    ["previous_day_admission_influenza_confirmed",
                     "previous_day_admission_influenza_confirmed_coverage",
                     "previous_day_deaths_influenza",
                     "previous_day_deaths_influenza_coverage"],
                "covid":
                    ["previous_day_admission_adult_covid_confirmed",
                     "previous_day_admission_adult_covid_confirmed_coverage",
                     "previous_day_admission_pediatric_covid_confirmed",
                     "previous_day_admission_pediatric_covid_confirmed_coverage",
                     "deaths_covid",
                     "deaths_covid_coverage"],
                "base": ["date",
                         "state"]}

SEASON_DRANGE = {2021: ("2021-09-01", "2022-06-01"),
                2022: ("2022-09-01", "2023-06-01"),
                2023: ("2023-08-01", datetime.now().strftime('%Y-%m-%d'))}


def data_api_fetch_by_query(disease="influenza",
                            state="CA",
                            season=2023,
                            order_by="date",
                            limit=CHUNK_LIMIT,
                            to_csv=False,
                            output_dir="../data"):
    params = {}

    cols = (f"{", ".join(DISEASE_COLS["base"])}, "
            f"{", ".join(DISEASE_COLS[disease])}")
    start_date = SEASON_DRANGE[season][0]
    end_date = SEASON_DRANGE[season][1]

    params["$select"] = cols
    params["$where"] = f"state = '{state}' AND date BETWEEN '{start_date}' AND '{end_date}'"
    params["$order"] = order_by
    params["$limit"] = limit

    response = requests.get(API, params=params)

    if response.ok:
        df = pd.read_csv(io.StringIO(response.text))
        last_modified = response.headers.get('Last-Modified', 0)
        if to_csv:
            last_mod_obj = datetime.strptime(last_modified, '%a, %d %b %Y %H:%M:%S %Z')
            filename = "HHS_daily-hosp_{state}_{season}_{disease}__{last_mod}.csv".format(
                state=state,
                season=season,
                disease=disease,
                last_mod=int(last_mod_obj.timestamp())
            )
            df.to_csv(os.path.join(output_dir, filename))
        return df, last_modified
    else:
        raise requests.HTTPError(response.text)


def data_api_fetch_all(order_by="date",
                       limit=CHUNK_LIMIT,
                       to_csv=True,
                       output_dir="../data"):

    params = {}
    cols = (f"{", ".join(DISEASE_COLS["base"])}, "
            f"{", ".join(DISEASE_COLS["influenza"])}, "
            f"{", ".join(DISEASE_COLS["covid"])}")

    params["$select"] = cols
    params["$order"] = order_by
    params["$limit"] = limit

    response = requests.get(API, params=params)

    if response.ok:
        df = pd.read_csv(io.StringIO(response.text))
        last_modified = response.headers.get('Last-Modified', 0)
        if to_csv:
            last_mod_obj = datetime.strptime(last_modified, '%a, %d %b %Y %H:%M:%S %Z')
            filename = "HHS_daily-hosp__{last_mod}.csv".format(
                last_mod=int(last_mod_obj.timestamp())
            )
            df.to_csv(os.path.join(output_dir, filename))
        return df, last_modified
    else:
        raise requests.HTTPError(response.text)


def data_api_fetch_lm():
    response = requests.get(API, params={"$select": "date", "$limit": 0, "$order": "date"})
    if response.ok:
        return response.headers.get('Last-Modified', 0)
    else:
        raise requests.HTTPError(response.text)


def check_data_against_lm(path):
    tdseconds = int(os.path.basename(path).split('__')[-1].rstrip('.csv'))
    file_lm_date = datetime.fromtimestamp(tdseconds)

    last_modified = data_api_fetch_lm()
    current_lm_date = datetime.strptime(last_modified, '%a, %d %b %Y %H:%M:%S %Z')

    return file_lm_date == current_lm_date
