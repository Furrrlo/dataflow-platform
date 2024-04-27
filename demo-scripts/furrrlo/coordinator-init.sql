CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- furrrlo
CREATE SERVER IF NOT EXISTS furrrlo FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (
    host 'postgres-worker',
    dbname 'furrrlogres',
    use_remote_estimate 'true',
    fetch_size '50000',
    batch_size '50000',
    fdw_tuple_cost '0.2'
);

CREATE USER MAPPING IF NOT EXISTS FOR postgres SERVER furrrlo
OPTIONS (user 'furrrlogres', password 'pwd.furrrlogres');

-- tossso
CREATE SERVER IF NOT EXISTS tossso FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (
    host 'tossso',
    port '5434',
    dbname 'tosssogres',
    use_remote_estimate 'true',
    fetch_size '50000',
    batch_size '50000',
    fdw_tuple_cost '0.2'
);

CREATE USER MAPPING IF NOT EXISTS FOR postgres SERVER tossso
OPTIONS (user 'tosssogres', password 'pwd.tosssogres');

-- fnufo
CREATE SERVER IF NOT EXISTS fnufo FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (
    host 'fnufo',
    port '5435',
    dbname 'fnufogres',
    use_remote_estimate 'true',
    fetch_size '50000',
    batch_size '50000',
    fdw_tuple_cost '0.2'
);

CREATE USER MAPPING IF NOT EXISTS FOR postgres SERVER fnufo
OPTIONS (user 'fnufogres', password 'pwd.fnufogres');
