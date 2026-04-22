FROM python:3.11-slim

WORKDIR /usr/app/dbt

RUN pip install --no-cache-dir dbt-bigquery==1.9.*

COPY . .

ENTRYPOINT ["dbt"]
CMD ["run"]
