CREATE TABLE esbatch (
  fileid BIGINT PRIMARY KEY,
  es_json JSON,
  rowinserttime timestamp default now(),
  errormsg varchar(512),
  retrytime timestamp default now()
);

