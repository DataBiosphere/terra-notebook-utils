import io
import os
from datetime import datetime
import json
from firecloud import fiss
from firecloud.errors import FireCloudServerError
import firecloud.api as fapi
import numpy as np
import pandas as pd

# User-set variables
BILLING_PROJECT_ID="biodata-catalyst"
WORKSPACE="TSV-AFY"
SUBDIRECTORY="\/case1\/"
TABLE_NAME="CRAMs"
# Make sure to escape the slashes in your SUBDIRECTORY variable. For instance, 
# if your files are in a folder called "testfiles" you will need to enter it 
# as "/\testfiles/\" or else Python will get angry.

# Call FireCloud API
try:
    bucket = os.environ["WORKSPACE_BUCKET"]
    response = fapi.list_entity_types(BILLING_PROJECT_ID, WORKSPACE)
    if response.status_code != 200:
        print("Error in Firecloud, check your billing project ID and the name of your workspace.")
    else:
        print("Firecloud has found your workspace!")
        directory = bucket + SUBDIRECTORY
except NameError:
    print("Caught a NameError exception. This probably means you didn't restart the kernal after"
          " running the first block of code (the one with all the imports). Run it again, restart"
          " the kernal, then try running every block of code (including the import one) again.")

# Append contents.txt with ls
!gsutil ls $directory > contentlocations.txt
# Append each line with their file names + full address 
# of where the files live in your google bucket
!cat contentlocations.txt | sed 's@.*/@@' > filenames.txt
!paste filenames.txt contentlocations.txt > combined.txt
# Set up header that Terra requires for data tables
# Simply doing this in Python, ie, 
#headerstring = "entity:" + TABLE_NAME + "_id\tfile_location"
# results in the tab being converted to a space. Not sure why
!touch temp.txt
!echo "entity:$TABLE_NAME""_id\tfile_location" >> temp.txt
!cat temp.txt combined.txt > final.tsv
# Clean up your directory
!rm filenames.txt contentlocations.txt temp.txt

# Upload TSV file as a Terra data table
response = fapi.upload_entities_tsv(BILLING_PROJECT_ID, WORKSPACE, "final.tsv", "flexible")
fapi._check_response_code(response, 200)
