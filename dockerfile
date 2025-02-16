# docker build -t pubmed-search-postgres-fastapi:test .
# docker run -d --name pubmed-search-postgres-fastapi --network pubmed_postgres_test -p 8001:8001 pubmed-search-postgres-fastapi:test

FROM python:3.9

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./.env /code/.env
COPY ./model /code/model
COPY ./app /code/app

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]