
# syntax=docker/dockerfile:1
FROM bytewax/bytewax:0.9.0-python3.8

WORKDIR /bytewax

RUN /venv/bin/pip install boto3

COPY . . 

RUN ["chmod", "+x", "./entrypoint.sh"]