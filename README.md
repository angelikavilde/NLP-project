# Overview

The project was based on flattening out the data from an xml file from AWS S3 bucket. Then the data had to be processed through NLP model to label the data from the 'affiliations' column to retrieve the extra data that might have been included. For example, some affiliations included the name of the medical institute through which we could improve the GRID id column as most of its data was missing. Data was labelled and processed through the institutes.csv file extracted also from the AWS bucket. This was done with the use of rapidfuzz as it allowed for string matching that wasn't exactly identical.

Some of the 'affiliations' column had also included extra data which could not be labelled and was retrieved with the use of regular expressions. For example, some affiliations included email or zip-code.

Some of the code was then improved for shortened speed of running with the use of multiprocessing on data columns for the NLP model. Cache and better data filtering was also later added to improve the performance speed for the data matching with RapidFuzz.

Later, `find_file.py` was added to track recently added files in the S3 bucket to retrieve only the recently added files of the correct file type and location.

## AWS

The following code was adapted into a Dockerfile for the AWS lambda function which was placed as a container into the AWS ECR and used within the state machine. The state machine shown below is shown to send an email upon starting the script and another one upon completion with an output from the lambda handler function located in the file `aws_lambda.py`. The state machine is activated using a trigger with set rule from AWS EventBridge once the correct file is added to the specified S3 bucket. The JSON for the configuration can be found below:

```{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": {
      "name": ["sigma-pharmazer-input"]
    },
    "object": {
      "key": [{
        "prefix": "angela"
      }]
    }
  }
}
```

<img src="https://github.com/angelikavilde/NLP-project/blob/main/screenshots/Screenshot%202023-08-23%20at%2013.49.09.png" width="440" height="400" alt="State machine on AWS showing an email upon the start of the machine and end with a lambda function that runs the pipeline">


<img src="https://github.com/angelikavilde/NLP-project/blob/main/screenshots/Screenshot%202023-08-23%20at%2013.50.07.jpg" width="520" height="280" alt="Emails received from the state machine">

## To run the script

`bash run_extract_transform.sh`






-----------------------------------------
-----------------------------------------
List of countries was downloaded from:

https://gist.github.com/keeguon/2310008
