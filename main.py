from datetime import datetime
import string
from typing import List
import databases
import sqlalchemy
from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import os
import urllib

host_server = 'localhost'
db_server_port = '5432'
database_name = 'covid'
db_username = 'postgres'
db_password = 'admin'
DATABASE_URL = 'postgresql://{}:{}@{}:{}/{}'.format(db_username,db_password,host_server,db_server_port,database_name)

database = databases.Database(DATABASE_URL)

metadata = sqlalchemy.MetaData()

covid_observations = sqlalchemy.Table(
    "covid_observations",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("sr_no", sqlalchemy.String),
    sqlalchemy.Column("observation_date", sqlalchemy.String),
    sqlalchemy.Column("province_or_state", sqlalchemy.String),
    sqlalchemy.Column("country_or_region", sqlalchemy.String),
    sqlalchemy.Column("last_update", sqlalchemy.String),
    sqlalchemy.Column("confirmed", sqlalchemy.String),
    sqlalchemy.Column("death", sqlalchemy.String),
    sqlalchemy.Column("recovered", sqlalchemy.String)
)


engine = sqlalchemy.create_engine(
    DATABASE_URL
)
metadata.create_all(engine)

class CovidObservation(BaseModel):
    id: int
    sr_no: str
    observation_date: str
    province_or_state: str
    country_or_region: str
    last_update: str
    confirmed: str
    death: str
    recovered: str

class CovidObservationIn(BaseModel):
    sr_no: str
    observation_date: str
    province_or_state: str
    country_or_region: str
    last_update: str
    confirmed: str
    death: str
    recovered: str

app = FastAPI(title = "REST API using FastAPI PostgreSQL Async EndPoints")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup():
    await database.connect()
    await delete_table()
    await insert_data_from_csv()

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()

@app.get("/top/confirmed", status_code = status.HTTP_200_OK)
async def get_top_confirmed(observation_date: str, max_results: int = 2):
    query = covid_observations.select()
    result = await database.fetch_all(query)
    data_per_country = {}
    countries_final_result = []

    for res in result:
        if res.observation_date == observation_date: #and float(res.confirmed) > 0:
            if res.country_or_region not in data_per_country:
                data_per_country[res.country_or_region] = {"country": res.country_or_region, "confirmed": 0, "deaths": 0, "recovered": 0}
            data_per_country[res.country_or_region]["confirmed"] += float(res.confirmed)
            data_per_country[res.country_or_region]["deaths"] += float(res.death)
            data_per_country[res.country_or_region]["recovered"] += float(res.recovered)
    
    if data_per_country:
        countries_final_result = sorted(data_per_country.values(), key=lambda d: d['confirmed'], reverse=True)

    final_result = {
        "observation_date": observation_date,
        "countries": countries_final_result[:max_results]
    }

    return final_result

async def insert_data_from_csv():
    import csv

    with open('covid_19_data.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count: #and line_count <= 99:
                formatted_observation_date = datetime.strptime(row[1], "%m/%d/%Y").strftime("%Y-%m-%d")
                query = covid_observations.insert().values(
                    sr_no=row[0],
                    observation_date=formatted_observation_date,
                    province_or_state=row[2],
                    country_or_region=row[3],
                    last_update=row[4],
                    confirmed=row[5],
                    death=row[6],
                    recovered=row[7]
                )
                await database.execute(query)
            line_count += 1
    
async def delete_table():
    query = covid_observations.delete()
    await database.execute(query)