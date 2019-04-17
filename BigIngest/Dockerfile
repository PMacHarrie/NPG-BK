#FROM python:3.6-alpine
FROM python:3.6

ADD lambda_bigingest.py /
ADD poller.py /
ADD extractors/ /extractors/

ENV AWS_ACCESS_KEY_ID=AKIAIYFRXG3MWD5T6CKA
ENV AWS_SECRET_ACCESS_KEY=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
ENV AWS_DEFAULT_REGION=us-east-1 
ENV ES_HOST='https://vpc-nde-dev2-jnk45bdfrsochphfpjinhf6dxu.us-east-1.es.amazonaws.com'
ENV ES_PORT='443'
ENV RD_HOST='nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com'
ENV RD_DBNM='nde_dev1'
ENV RD_USER='nde_dev1'
ENV RD_PSWD='nde'
#ENV SQS_BIG: 'https://sqs.us-east-1.amazonaws.com/784330347242/BigIngestQ'
ENV incomingdir='/tmp'

#RUN apk add build-base
#RUN apk add postgresql-dev
RUN pip3 install psycopg2
RUN pip3 install elasticsearch
RUN pip3 install boto3
RUN pip3 install h5py 
RUN pip3 install netCDF4
RUN pip3 install certifi

CMD ["python", "./poller.py"]


