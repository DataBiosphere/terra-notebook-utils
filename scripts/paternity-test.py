import io
import os
import json # might not be needed
from firecloud import fiss
from firecloud.errors import FireCloudServerError
import firecloud.api as fapi
import numpy as np
import pandas as pd
import logging
#import re <-- doesn't work on Terra

# User-set variables
BILLING_PROJECT_ID="biodata-catalyst"
WORKSPACE="TSV-AFY"
SUBDIRECTORY="\/case2\/"
TABLE_NAME="CRAMs_and_CRAIs" #Do not include spaces or weird characters
PARENT_FILETYPE="cram"
# If your filenames are like this, please set INCLUDE_PARENT_EXTENSION to True:
# NWD119844.CRAM
# NWD119844.CRAM.CRAI
# If you filenames are like this, please set INCLUDE_PARENT_EXTENSION to False:
# NWD119844.CRAM
# NWD119844.CRAI
# No quotation marks for this variable, just True or False
INCLUDE_PARENT_EXTENSION = True
# Make sure to escape the slashes in your SUBDIRECTORY variable. 
# For instance, if your files are in a folder called "testfiles" you will need to
# enter it as "/\testfiles/\" or else Python will get angry.

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

# Do magic to create a TSV file
# We don't have the option to use Python's parsing directly, as the notebook VM is seperate 
# from the Google Cloud bucket where the data is actually stored, and we cannot run arbitrary 
# code in a Google Cloud bucket. This forces us to parse the output of gsutil ls. 
# Parsing ls is always a little dodgy, but we really don't have any other option. 
# As a result, if your filenames contain non-ascii (ie, stuff besides A-Z, a-z, underscores, and dashes)
# or bizarre characters (ie, newlines) there is a chance this will not work as expected.

# Pandas dataframe method
# Note: I am sure there is a more efficient way of doing this!

def baseID(filename_string, child_extension):
    global PARENT_FILETYPE
    global INCLUDE_PARENT_EXTENSION
    if INCLUDE_PARENT_EXTENSION:
        fileID = filename_string.replace("."+child_extension,"")
    else:
        fileID = filename_string.replace("."+PARENT_FILETYPE+"."+child_extension,"")
    return fileID

# Get location of everything and their file names
logging.info("Querying Google and writing results...")
!gsutil ls $directory > contentlocations.txt
!cat contentlocations.txt | sed 's@.*/@@' > filenames.txt

# Import everything
logging.info("Constructing dataframe...")
data={}
this_file=open("contentlocations.txt", "r")
lineslocation = this_file.read().splitlines()
this_file.close()
that_file=open("filenames.txt", "r")
linesfilename = that_file.read().splitlines()
data['filename'] = linesfilename
data['location'] = lineslocation #here in order to put columns in a particular order without reassigning later

# create dataframe
df = pd.DataFrame(data)

# Split on file extension
df['FileType'] = df.filename.str.rsplit('.', 1).str[-1]

# Count file extensions to see how many child file types there are
unique_children = pd.unique(df[['FileType']].values.ravel("K")) #K might not be fastest option, do more testing
unique_children = unique_children[unique_children!=PARENT_FILETYPE]

# Play with strings to get basic ID, needed to link parents and children
# There's probably a faster way to do this?

logging.info("Manipulating dataframe...")

i = 0
for child_extension in unique_children: #loop once per child extension
    i=i+1
    logging.info("\t* Dealing with %s (child %d out of %d)" % (child_extension, i, len(unique_children)))
    df[child_extension] = ""
    df[child_extension+"_location"] = ""
    output_list={}
    for index_label, row_series in df.iterrows():
        # On first iteration, iterate over parent files
        # Only do once to avoid wasting time repeating this constantly
        if (df.at[index_label,'FileType'] == PARENT_FILETYPE) & (i==1):
            parent_baseID = baseID(row_series['filename'], child_extension)
            df.at[index_label,'ID'] = parent_baseID
        # Only iterate over children that match the child_extension
        # This avoids overwriting if there's more than one child_extension
        elif df.at[index_label,'FileType'] == child_extension:
            child_baseID = baseID(row_series['filename'], child_extension)
            df.at[index_label,'ID'] = child_baseID
            #output_list.update({baseID(row_series['filename'], child_extension) : df.at[index_label,'FileType']})
        else:
            pass

logging.info("Matching children and parents...")
i = 0 #only used for logging
# Iterate yet again to match children with their parents
for child_extension in unique_children: #loop once per child extension
    i=i+1
    logging.info("\t* Dealing with %s (child %d out of %d)" % (child_extension, i, len(unique_children)))
    for index_label, row_series in df.iterrows():
        if(df.at[index_label,'FileType'] == PARENT_FILETYPE):
            # Find this parent's child
            # Child might be above parent so we can't just start from index of the parent
            for index_label_inception, row_series_inception in df.iterrows():
                #print("Outer iter %d, inner iter %d parent %s checking if %s is its child" % (index_label, index_label_inception, df.at[index_label,'filename'], df.at[index_label_inception,'filename']))
                if index_label != index_label_inception: #needed to preventing it find itself
                    if(df.at[index_label,'ID'] == df.at[index_label_inception,'ID']):
                        # Child found!
                        #print("    Found "+df.at[index_label_inception,'filename']+" to be child")
                        df.at[index_label,child_extension] = df.at[index_label_inception,'filename']
                        df.at[index_label,child_extension+"_location"] = df.at[index_label_inception,'location']
                        break
            if(df.at[index_label,child_extension] == ""):
                logging.warning("Could not find child of type %s for parent %s" % (child_extension, df.at[index_label, 'FileType']))
print(df)
logging.info("Cleaning up dataframe...")           
# Iterate one more time to delete child rows
# Because children could appear above their parents, deleting during the above iteration could mess things up
df.drop(columns=['ID'], inplace=True)
df.rename(columns = {'FileType':'parent_file_ext'}, inplace = True)
for index_label, row_series in df.iterrows():
        if(df.at[index_label,'parent_file_ext'] != PARENT_FILETYPE):
            df.drop([index_label], inplace=True)
df.reset_index(inplace=True, drop=True)
logging.info("Finished")

# Generate TSV file from Dataframe

#!rm final.tsv
# ^ Uncomment above line if you will be running this block more than once

df.to_csv("dataframe.tsv", sep='\t')
# Format resulting TSV file to play nicely with Terra 
with open('dataframe.tsv', "r+") as file1:
    header = file1.readline()
    everything_else = file1.readlines()
    file1.close()
full_header="entity:"+TABLE_NAME+"_id"+header
with open('final.tsv', "a") as file2:
    file2.write(full_header)
    for string in everything_else:
        file2.write(string)
    file2.close()

# Clean up
!rm dataframe.tsv

# Upload TSV file as a Terra data table
response = fapi.upload_entities_tsv(BILLING_PROJECT_ID, WORKSPACE, "final.tsv", "flexible")
fapi._check_response_code(response, 200)
