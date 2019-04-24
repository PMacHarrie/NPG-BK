--TODO: This defines only part of the NDE-in-the-Cloud 'nde_dw' schema. Will add additional tables from this database as they are needed.

DROP INDEX IF EXISTS if_filename_index;
DROP TABLE IF EXISTS if_objectevent;

CREATE TABLE if_objectevent (
	if_filedetectedtime		timestamp without time zone,
	if_filesourcecreationtime	timestamp without time zone,
	if_filereceiptcompletiontime	timestamp without time zone,
	if_endobservationtime		timestamp without time zone,
	if_filename			character varying(512),
	if_filecompletionstatus		character varying(12),
	if_filepullstarttime		integer,
	if_filemessagecreatetime	timestamp without time zone
);

CREATE INDEX if_filename_index ON if_objectevent (if_filename);

DROP INDEX IF EXISTS if_irpje;
DROP INDEX IF EXISTS iirid_index;
DROP TABLE IF EXISTS if_interfaceevent;

CREATE TABLE if_interfaceevent (
	if_interfacerequestpolljobenqueue 	timestamp without time zone,
	if_interfacerequestpolljobdequeue 	timestamp without time zone,
	if_interfacerequestpolljobcomplete 	timestamp without time zone,
	if_requestprocessedcnt 			integer 			DEFAULT 0,
	if_interfacerequest 			character varying(255),
	if_interfacerequestid 			character varying(255)
);

CREATE INDEX if_irpje ON if_interfaceevent (if_interfacerequestpolljobenqueue);
CREATE INDEX iirid_index ON if_interfaceevent (if_interfacerequestid);
