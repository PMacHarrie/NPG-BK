CREATE TABLE ingestlog (
  il_id BIGINT PRIMARY KEY,
  productid BIGINT,
  fileid BIGINT,
  filename VARCHAR(255),
  ilstatus VARCHAR(32),
  ilfailurereason VARCHAR(1024),
  ilmetrics_json JSON,
  ilresource VARCHAR(255),
  rowinserttime timestamp default now()
);
