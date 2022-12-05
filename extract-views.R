## SUBJECT DATE
DATE_PARAM="2022-10-20"

date <- as.Date(DATE_PARAM, "%Y-%m-%d")

#install.packages('httr')
#install.packages('jsonlite', 'lubridate')
#install.packages('testit')

library(httr)
library(aws.s3)
library(jsonlite)
library(lubridate)
library(testit)

# See https://wikimedia.org/api/rest_v1/#/Edited%20pages%20data/get_metrics_edited_pages_top_by_edits__project___editor_type___page_type___year___month___day_
url <- paste(
  "https://wikimedia.org/api/rest_v1/metrics/edited-pages/top-by-edits/en.wikipedia/all-editor-types/content/",
  format(date, "%Y/%m/%d"), sep='')

print(paste('Requesting REST API URL: ', url, sep=''))
wiki.server.response = GET(url)

wiki.response.status = status_code(wiki.server.response)
wiki.response.body = content(wiki.server.response, 'text')

print(paste('Wikipedia REST API Response body: ', wiki.response.body, sep=''))
print(paste('Wikipedia REST API Response Code: ', wiki.response.status, sep=''))

if (wiki.response.status != 200){
  print(paste("Recieved non-OK status code from Wiki Server: ",
              wiki.response.status,
              '. Response body: ',
              wiki.response.body, sep=''
  ))
}


# access AWS keys
keyfile = list.files(path=".", pattern="accessKeys.csv", full.names=TRUE)
if (identical(keyfile, character(0))){
  stop("ERROR: AWS key file not found")
} 

keyTable <- read.csv(keyfile, header = T) # *accessKeys.csv == the CSV downloaded from AWS containing your Access & Secret keys
AWS_ACCESS_KEY_ID <- as.character(keyTable$Access.key.ID)
AWS_SECRET_ACCESS_KEY <- as.character(keyTable$Secret.access.key)

#activate
Sys.setenv("AWS_ACCESS_KEY_ID" = AWS_ACCESS_KEY_ID,
           "AWS_SECRET_ACCESS_KEY" = AWS_SECRET_ACCESS_KEY,
           "AWS_DEFAULT_REGION" = "eu-west-1") 


## Parse the wikipedia response and write the parsed string to "Bronze"

# First, we are extracting the top edits from the server's response

wiki.response.parsed = content(wiki.server.response, 'parsed')
top.edits = wiki.response.parsed$items[[1]]$results[[1]]$top

# Convert the server's response to JSON lines
current.time = Sys.time() 
json.lines = ""
for (page in top.edits){
  record = list(
    title = page$page_title[[1]],
    edits = page$edits,
    date = format(date, "%Y-%m-%d"),
    retrieved_at = current.time
  )
  
  json.lines = paste(json.lines,
                     toJSON(record,
                            auto_unbox=TRUE),
                     "\n",
                     sep='')
}

# Save the Top Edits JSON lines as a file and upload it to S3

JSON_LOCATION_BASE='data/views'
dir.create(file.path(JSON_LOCATION_BASE), showWarnings = TRUE)

json.lines.filename = paste("views-", format(date, "%Y-%m-%d"), '.json',
                            sep='')
json.lines.fullpath = paste(JSON_LOCATION_BASE, '/', 
                            json.lines.filename, sep='')

write(json.lines, file = json.lines.fullpath)

put_object(file = json.lines.fullpath,
           object = paste('datalake/views/', 
                          json.lines.filename,
                          sep = ""),
           bucket = bucket,
           verbose = TRUE)


