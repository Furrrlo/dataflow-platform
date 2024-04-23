CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- furrrlo
CREATE SERVER IF NOT EXISTS furrrlo FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'furrrlo', port '5433', dbname 'furrrlogres');

CREATE USER MAPPING IF NOT EXISTS FOR postgres SERVER furrrlo
OPTIONS (user 'furrrlogres', password 'pwd.furrrlogres');

-- tossso
CREATE SERVER IF NOT EXISTS tossso FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'tossso', port '5434', dbname 'tosssogres');

CREATE USER MAPPING IF NOT EXISTS FOR postgres SERVER tossso
OPTIONS (user 'tosssogres', password 'pwd.tosssogres');

-- fnufo
CREATE SERVER IF NOT EXISTS fnufo FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'fnufo', port '5435', dbname 'fnufogres');

CREATE USER MAPPING IF NOT EXISTS FOR postgres SERVER fnufo
OPTIONS (user 'fnufogres', password 'pwd.fnufogres');
