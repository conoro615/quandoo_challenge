FROM python:3.10-slim-buster

RUN apt-get update \
    && apt-get install -y --no-install-recommends

WORKDIR /dbt

# Copy main files for dbt to function
COPY models models
COPY tests tests
COPY dbt_project.yml dbt_project.yml
COPY profiles.yml profiles.yml

# Install the dbt Postgres adapter
RUN pip install --upgrade pip
RUN pip install dbt-postgres


# Install dbt dependencies 
CMD dbt deps && dbt build --profiles-dir . && sleep infinity