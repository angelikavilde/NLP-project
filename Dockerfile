FROM amazon/aws-lambda-python

WORKDIR ${LAMBDA_TASK_ROOT}

COPY requirements.txt .

RUN pip install -r requirements.txt

RUN python -m spacy download en_core_web_sm

COPY aws_lambda.py .
COPY countries.json .
COPY institutes_data.csv .
COPY extract.py .
COPY transform.py .
COPY find_file.py .
COPY addresses_data.csv .

CMD [ "aws_lambda.lambda_handler" ]
