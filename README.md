# Overview

The project was based on flattening out the data from an xml file from AWS S3 bucket. Then the data had to be processed through NLP model to label the data from the 'affiliations' column to retrieve the extra data that might have been included. For example, some affiliations included the name of the medical institute through which we could improve the GRID id column as most of its data was missing. Data was labelled and processed through the institutes.csv file extracted also from the AWS bucket. This was done with the use of rapidfuzz as it allowed for string matching that wasn't exactly identical.

Some of the 'affiliations' column had also included extra data which could not be labelled and was retrieved with the use of regular expressions. For example, some affiliations included email or zip-code.

Some of the code was then improved for shortened speed of running with the use of multiprocessing on data columns for the NLP model.

## To run the script:

`bash run_extract_transform.sh`







List of countries was downloaded from:

https://gist.github.com/keeguon/2310008