/* ************************************************************ */
/* name: CREATE_JOBTEMP.sql                               */
/* revised: 20130808 pgm, creation                              */
/* revised: 20160318 pgm, add index                             */
/* ************************************************************ */
DROP INDEX JTS_IDX;
DROP INDEX JS_S_UPK_IDX;
DROP TABLE jobTemp;
DROP INDEX PJT_S_IDX;
DROP TABLE potJobTemp;
DROP INDEX PJT2_S_UPK_IDX;
DROP TABLE potjobtemp2;

CREATE TABLE jobTemp (
  sessionId BIGINT,
  updatePrimaryKey BIGINT
  );
CREATE INDEX JT_S_UPK_IDX ON JOBTEMP(SESSIONID,UPDATEPRIMARYKEY);

create table potJobTemp (
sessionId BIGINT,
v_prid BIGINT,
v_jobstart timestamp(6),
v_jobend  timestamp(6),
v_jobclass   BIGINT,
v_jobpriority BIGINT,
v_prproductcoverageinterval_ds  INTERVAL DAY TO SECOND(5),
v_prruninterval_ds INTERVAL DAY TO SECOND(5),
v_numjobs  BIGINT,
v_fst timestamp(6),
v_fet timestamp(6),
v_prwaitforinputinterval_ds INTERVAL DAY TO SECOND(5)
);

create index PJT_S_IDX ON POTJOBTEMP(SESSIONID);

create table potjobtemp2 (
sessionId BIGINT,
updateprimarykey BIGINT
);

create index PJT2_S_UPK_IDX ON POTJOBTEMP2(SESSIONID, UPDATEPRIMARYKEY);

