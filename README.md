# fastapi_postgresql
FastAPI with PostgreSQL

Prerequisite:
1. PortgreSQL

How to run the application.
1. Configure your PortgreSQL in file main.py\
Example:\
  host_server = 'localhost'\
  db_server_port = '5432'\
  database_name = 'covid'\
  db_username = 'postgres'\
  db_password = 'admin'
2. Activate virtualenv using command\
source env/Scripts/activate 
4. Start the application using command\
uvicorn main:app --reload 
6. To access the application go to http://127.0.0.1:8000/docs
