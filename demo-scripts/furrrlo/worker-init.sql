CREATE EXTENSION IF NOT EXISTS postgres_fdw;

CREATE SERVER IF NOT EXISTS coordinator FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (
    host 'furrrlo',
    dbname 'postgres',
    use_remote_estimate 'true',
    fetch_size '50000',
    fdw_tuple_cost '0.2'
);

CREATE USER MAPPING IF NOT EXISTS FOR furrrlogres SERVER coordinator
OPTIONS (user 'postgres', password 'pwd.postgres');
