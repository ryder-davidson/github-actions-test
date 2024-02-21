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
#' @param down_dir character string. The directory path to download to.
#' @param down_filename character string. The filename to download to.
#' @param fields character string. The filename to download to.
#' @param order character string. The filename to download to.
#' @param limit character string. The filename to download to.
#'
#' @return an integer. 0 for success and non-zero for failure.
#' @export
#'
#' @examples
#' hhs_hosp_state_down(down_dir = "~/Downloads", down_filename = NULL)
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
  if (length(query) == 0) {
    response <- httr::GET(API)
  } else {
    response <- httr::GET(API, query=query)
  }

  # Check response status
  if (response$status_code == 200) {
    posix_timestamp <- as.POSIXct(response$headers$`last-modified`, format = FROM_TIMESTAMP_FMT, tz = "GMT")

    if (is.null(down_filename)) {
      fmt_timestamp <- format(posix_timestamp, TO_TIMESTAMP_FMT)
      down_filename <- paste(DEF_FILE_BASE, "__", fmt_timestamp, ".csv", sep = "")
    }

    filepath <- file.path(down_dir, down_filename)
    data <- read.csv(text = httr::content(response, "text"))
    if ("date" %in% colnames(data)) {
      data$date <- as.Date(data$date, format = "%Y-%m-%d")
    }

    write.csv(data, filepath, row.names = FALSE)

    # Check if file was created
    if (file.exists(filepath)) {
      cat("\nData Saved At:", filepath, "\n\n")
      return_data$download_path <- filepath
      return_data$last_modified <- posix_timestamp
      return_data$out_flag <- 0

      file_pattern <- paste(DEF_FILE_BASE, "__[0-9]+\\.csv$", sep = "")
      files <- list.files(down_dir, pattern = file_pattern)
      for (file in files) {
        if (file != down_filename) {
          file.remove(file.path(down_dir, file))
          cat("Removing File:", file, "\n")
        }
      }
    }

    return(return_data)
  } else {
    cat(httr::content(response, as="text"))
    return(return_data)
  }
}


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

result <- fetch_hhs_data(down_dir = "data", limit = 10)
posix_result <- filename_timestamp_to_posix(result$download_path)
