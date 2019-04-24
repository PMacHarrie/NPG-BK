/* ************************************************************ */
/* name: CREATE_RETRIEVE_DPRS.sql                               */
/* revised: 20110713 lhf, creation                              */
/*          20120229 lhf, remove GLOBAL TEMPORARY NDE-481       */
/*          20120523 lhf, build 4/5 updates, NDE-607            */
/* ************************************************************ */
DROP TABLE RETRIEVE_DPRS;
CREATE TABLE RETRIEVE_DPRS (
  DPRREQID NUMBER(15),
  DPRCOMPRESSIONTYPE VARCHAR(25),
  DPRCHECKSUMTYPE VARCHAR2(12),
  FILEID NUMBER(15),
  DICLASS NUMBER(6),
  DIPRIORITY NUMBER(38),
  PRODUCTHOMEDIRECTORY VARCHAR2(255),
  FILENAME VARCHAR2(255),
  DPRREQENQUEUETIME TIMESTAMP(6));
quit
