#!/bin/bash
#-------------------------------------------------------------------------------
# Script     : data_api_fetchall.sh.
# Description: Fetch data from healthdata.gov.
#-------------------------------------------------------------------------------

readonly STATUS_OK=0
readonly STATUS_ERROR=1
status=$STATUS_ERROR

if [ -d "$1" ]; then
    data_dir=$1
else
    echo "Directory $1 does not exist"
    exit $STATUS_ERROR
fi

API="https://healthdata.gov/resource/g62h-syeh.csv";
COLS="date,state,previous_day_admission_influenza_confirmed,previous_day_admission_influenza_confirmed_coverage,previous_day_deaths_influenza,previous_day_deaths_influenza_coverage,previous_day_admission_adult_covid_confirmed,previous_day_admission_adult_covid_confirmed_coverage,previous_day_admission_pediatric_covid_confirmed,previous_day_admission_pediatric_covid_confirmed_coverage,deaths_covid,deaths_covid_coverage";
ORDER="date";
LIMIT="100";
API_QUERY="${API}?\$select=${COLS}&\$order=${ORDER}&\$limit=${LIMIT}";

time {
    echo
    echo "----------------------"
    echo "EXECUTING API REQUEST:"
    echo "----------------------"
    echo "$API_QUERY";
    echo

    response=$(curl -D "$data_dir/headers.txt"  "$API_QUERY");
    res_code=$(grep "^HTTP" "data/headers.txt" | awk '{print $2}')
    if [ "$res_code" -eq 200 ]; then
        timestamp=$(grep "^Last-Modified" "$data_dir/headers.txt" | awk -F ": " '{print $2}' | awk '{month_abv = $3; months = "JanFebMarAprMayJunJulAugSepOctNovDec"; month_num = (index(months, month_abv) + 2) / 3; printf "%s%02d%s%s", $2, month_num, substr($4, 3, 2), $5}' | tr -d ':')

        filename="$data_dir/HHS_daily-hosp_state__$timestamp.csv"
        echo "$response" | awk '{gsub("T00:00:00.000", "", $1); print}' > "$filename"

        status=$STATUS_OK
        echo
        echo "Data Saved At: $data_dir/HHS_daily-hosp_state__$timestamp.csv"
    else
        echo
        echo "$response"
    fi

    echo
    echo "----------------------"
    echo "CLEANING UP TEMP FILES"
    echo "----------------------"

    if [ -f "$data_dir/headers.txt" ]; then
        rm "$data_dir/headers.txt"
        echo "Removing File: $data_dir/headers.txt"
    fi

    echo
    echo "---------------------"
    echo "SCRIPT EXECUTION TIME"
    echo "---------------------"
}

exit "$status"
