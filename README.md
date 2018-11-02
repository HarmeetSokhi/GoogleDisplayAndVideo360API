# GoogleDisplayAndVideo360API
#!/usr/bin/python
#****************************************************************************************
#    DBM  Python Script for  Fetching data from Doubleclick Bid Manager/ Google Display and Video  360
#   --------------------------------------------
#
#    Notes:
#
#    1. This script fetches data use DBM Google API for the XXXXX Client.
#       You can see the variables below for the details of the API
#    2. Script take JSON as a input from the config_file
#    3. Once the data is received it writes to S3 (AWS).Please see the Variables for
#       the path details
#    4. Once Data is written to S3 it writes it to the Redshift staging table.Staging
#       table is truncated before writing the data
#    5. Data is inserted into Master table after the above step
#    6. It overrides 30 days of data. You can change it based on your requirement
#
#    Version:
#      Please check Git for Change Management & Details.
#      18/10/2018
#*******************************************************************************************