CREATE EXTENSION IF NOT EXISTS postgres_fdw;

CREATE SERVER IF NOT EXISTS coordinator FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'furrrlo', dbname 'postgres');

CREATE USER MAPPING IF NOT EXISTS FOR tosssogres SERVER coordinator
OPTIONS (user 'postgres', password 'pwd.postgres');
