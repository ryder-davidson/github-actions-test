COLS <- list("date", "state", "previous_day_admission_influenza_confirmed",
          "previous_day_admission_influenza_confirmed_coverage",
          "previous_day_deaths_influenza",
          "previous_day_deaths_influenza_coverage",
          "previous_day_admission_adult_covid_confirmed",
          "previous_day_admission_adult_covid_confirmed_coverage",
          "previous_day_admission_pediatric_covid_confirmed",
          "previous_day_admission_pediatric_covid_confirmed_coverage",
          "deaths_covid", "deaths_covid_coverage")

DEF_FILE_BASE <- "HHS_daily-hosp_state"

FROM_TIMESTAMP_FMT <- "%a, %d %b %Y %H:%M:%S GMT"

TO_TIMESTAMP_FMT <- "%y%m%d%H%M%S"

API <- "https://healthdata.gov/resource/g62h-syeh.csv"

#' Download daily state-level HHS PROTECT hospitalization admission data
#' to a CSV.
#'
#' Function returns the filepath where the HHS csv file was downloaded to,
#' the date the dataset was last modified (as reported by healthdata.gov),
#' and a flag corresponding to the status code of the API call. The data
#' fetched through `fetch_hhs_data` is accessed through the HealthData.gov API,
#' supported by the Socrata framework. As such, the API utilizes SoQL
#' syntax to construct a DB query and retrieve the relevant data. The
#' function signature for `fetch_hhs_data` wraps the Socrata API,
#' constructing a SoQL query of the form:
#'      SELECT `fields` FROM `API`
#'      WHERE `conditions`
#'      ORDER BY `order` ASC
#'      LIMIT `limit`
#'
#' NOTE: When no `down_filename` is provided, the function creates a filename
#' HHS_daily-hosp_state__<last_modified>.csv, where `last_modified` is a POSIX
#' timestamp of the form: YYmmddHHMMSS.
#'
#' @param down_dir character string. The directory path to download to.
#' @param down_filename character string. The filename to download to.
#' @param fields character vector. Fields included in GET query.
#' @param order character string. Field to order the returned dataset on.
#' @param limit integer. Maximum number of records returned by GET query.
#' @param conditions character string. WHERE clause used in DB query.
#'
#' @return list. Named list containing: download_path, last_modified, and out_flag
#' @export
#'
#' @examples
#' hhs_hosp_state_down(down_dir = "data")
#' hhs_hosp_state_down(down_dir = "data",
#'                      fields=c("date", "state", deaths_covid"),
#'                      conditions="state == 'CA' AND deaths_covid IS NOT NULL")
#'
fetch_hhs_data <- function(down_dir="~",
                           down_filename=NULL,
                           fields=COLS,
                           order="date",
                           limit=1000000,
                           conditions=NULL) {

  return_data <- list(download_path=NULL, last_modified=NULL, out_flag=1)

  if (!dir.exists(down_dir)) {
    cat("'", down_dir, "'", "directory does not exist\n")
    return(return_data)
  }

  query <- list()
  if (!is.null(fields)) {
    query$`$select` <- paste(fields, collapse = ",")
  }
  if (!is.null(order)) {
    query$`$order` <- as.character(order)
  }
  if (!is.null(limit)) {
    query$`$limit` <- as.character(limit)
  }
  if (!is.null(conditions)) {
    query$`$where` <- as.character(conditions)
  }

  # Perform API request
  # If query dictionary has been populated, pass this as an argument to GET call
  if (length(query) == 0) {
    response <- httr::GET(API)
  } else {
    response <- httr::GET(API, query=query)
  }

  # Check response status
  if (response$status_code == 200) {
    posix_timestamp <- as.POSIXct(response$headers$`last-modified`, format = FROM_TIMESTAMP_FMT, tz = "GMT")

    # Generate default download_path of the form:
    # HHS_daily-hosp_state__<last_modified>.csv

    # This default naming protocol can be used in conjunction with the function
    # `filename_timestamp_to_posix` to read in the `last_modified` metadata pertaining
    # to a dataset, and determine if a more recent dataset is available from the
    # healthdata.gov API.
    if (is.null(down_filename)) {
      fmt_timestamp <- format(posix_timestamp, TO_TIMESTAMP_FMT)
      down_filename <- paste(DEF_FILE_BASE, "__", fmt_timestamp, ".csv", sep = "")
    }

    filepath <- file.path(down_dir, down_filename)
    data <- read.csv(text = httr::content(response, "text"))

    # "https://healthdata.gov/resource/g62h-syeh.csv" returns the `date` column as a
    # datetime type. PROF requires the `date` column to be a date type, not datetime.
    # As such, if `date` is included, it is cast into the form: YYYY-mm-dd.
    if ("date" %in% colnames(data)) {
      data$date <- as.Date(data$date, format = "%Y-%m-%d")
    }

    write.csv(data, filepath, row.names = FALSE)

    if (file.exists(filepath)) {
      cat("\nData Saved At:", filepath, "\n\n")
      return_data$download_path <- filepath
      return_data$last_modified <- posix_timestamp
      return_data$out_flag <- 0

      # Clean out existing dataset. For now, this only looks for csv files that
      # have been generated through the function's default filepath naming
      # conventions, viz. using the "__<last_modified>.csv" as an identifier for
      # possible matches.
      # This functionality has been commented out pending approval.

#       file_pattern <- paste(DEF_FILE_BASE, "__[0-9]+\\.csv$", sep = "")
#       files <- list.files(down_dir, pattern = file_pattern)
#       for (file in files) {
#         if (file != down_filename) {
#           file.remove(file.path(down_dir, file))
#           cat("Removing File:", file, "\n")
#         }
      }
    }

    return(return_data)
  } else {
    cat(httr::content(response, as="text"))
    return(return_data)
  }
}

#' Convert POSIX timestamp from filename to POSIXct object.
#'
#' This function extracts a POSIX timestamp from a filename following a specific pattern,
#' then converts it to a POSIXct object.
#'
#' @param filepath Character string. The filepath containing the filename with the POSIX timestamp.
#'
#' @return A POSIXct object representing the timestamp extracted from the filename.
#' @export
#'
#' @examples
#' filepath <- "HHS_daily-hosp_state__210226120000.csv"
#' posix_timestamp <- filename_timestamp_to_posix(filepath)
#' posix_timestamp
#'
filename_timestamp_to_posix <- function(filepath) {
  pattern <- paste(DEF_FILE_BASE, "__[0-9]{12}\\.csv$", sep = "")
  filename <- regmatches(filepath,regexpr(pattern, filepath))
  if (length(filename) != 0) {
    lm_timestamp <- regmatches(filename,regexpr("[0-9]{12}", filename))
    posix_timestamp <- as.POSIXct(lm_timestamp, format = TO_TIMESTAMP_FMT, tz = "GMT")
    return(posix_timestamp)

  } else {
    cat("ERROR: Improper filename structure.\nCannot extract POSIX timestamp.")
    return()
  }
}
