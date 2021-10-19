# __Enrichment Tool__

## Methodology of tool

### 1. Determine Intent of enrichment
- Process user entered arguments

### 2. Collect leads to enrich
- Salesforce
- Redshift

### 3. Initialize modules
- Modules in /modules folder under app '/Enrichement\ Tool/modules/'

### 4. Clean Data
- Clean Data for bad phone number, street, state, zip, ect...

### 5. Collect data from various sources
- Google Places(lat, lng)
- Facebook
- Yelp
- FiveStars Nearby(redshift/fs postgres)
- Foursquare
- Walking Score
- Transit Score

### 6. Validate Data
- Validation is a series of negative filters
    - Excluded company names(mostly enterprise and some DNCs)
    - Propability match of Facebook page search result
    - Category of lead from Facebook does not contain X __at all__
    - Category of lead from Facebook does not contain __exactly__ X
    - Category of lead from Yelp does not contain X __at all__
    - Category of lead from Yelp does not contain __exactly__ X

### 7. Update/Create Record to Salesforce
- Salesforce API to update/create records


# Command Line Implementation:

### Usage
"""
-------------------------------------------------------------------------------
Usage:
    app.py [FILE] [PURPOSE] [DAYS_SINCE_ENRICHMENT] [--limit=N] [-hvdaerlt] [EMAIL] [--split=N]

ipython ex: ipython app.py -- rekindle.py "rekindle for outside" --limit=20 -d -v
    Above processes 20 rekindle leads in decending order with verbose mode on
    the marketing initiative will be "rekindle for outside <today's date>"

python ex: same as above but you can exclude the two hyphens '--'

Help ex: ipython app.py -- -h or python app.py -h

Arguments:
    EMAIL                  your email if you want to be notified about success/fail
                           currently if you add an email you need to also add the
                           DAYS_SINCE_ENRICHMENT value before it, just use 30
    FILE                   file that contains the query to salesforce leads located in
                           ~app/enrichments/
    PURPOSE                purpose for enrichment mapped to "marketing initative" field
                           you can use this field to string search in salesforce
                           (field can be overwritten, typically used for temp reasons)
    DAYS_SINCE_ENRICHMENT  how many days to delay from hitting realvalidation
                           default is 30 days (must be greater than 0)

Options:
    -h                  help
    -v                  verbose mode
    -l                  rebuild the learning model pkl
    -d                  decending query order by company name
    -a                  ascending query order by company name
    -e                  error troubleshooting mode, set's processing to 1 record and
                        query limit to 5 records
    -r                  indicates script to run in rekindle mode
    --limit=N           limit followed by number of records to limit in query
    -t                  This is for testing purposes so that the whole setup process runs faster,
                        it will first use the pre-set facebook tokens then if they have expired it
                        will grab only one from facebook
    --split=N           this allows you to split the query into 3 and process the N segment of query
-------------------------------------------------------------------------------
"""

# IMPORTANT
### config.json for all passwords and logins looks like this
- Please use Salesforce Sandbox credentials rather than production Salesforce credentials
- To get a key for Facebook, you must create a developer app and then verify the keys at
    `developers.facebook.com/tools/access_token/`

{
    "facebook": {
        "likes": "fan_count",
        "username": "",
        "password": "",
        "keys": [
            "key",
            "key",
        ]
    },
    "redshift": {
        "host": "",
        "dbname": "",
        "user": "",
        "password": "",
        "port": ""
    },
    "salesforce": {
        "username": "",
        "password": "",
        "token": ""
    }
}
