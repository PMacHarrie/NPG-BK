/* ************************************************************ */
/* name: ALTER_DISJOBEXTHOST.sql				*/
/* revised: 20130816 pgm, creation                              */
/* Identify as needed for distribution performance		*/
/*								*/
/* Install instructions						*/
/* It is recommended that this only be installed in Performance */
/* test modes, and the production mode				*/
/* It is preferrable to leave the constraints in place for 	*/
/* development and functional tests				*/
/*								*/
/* The DHS Distribution Servers (Factory and Processing) should */
/* all be brought down before executing this add-on		*/
/*								*/
/* ************************************************************ */
alter table DISTRIBUTIONJOBEXTERNALHOST
   drop constraint FK_DISTRIBU_DIJEH_DN__DISTRIBU;

alter table DISTRIBUTIONJOBEXTERNALHOST
   drop constraint FK_DISTRIBU_DJ_DJEH_R_DISTRIBU;

drop index DIJEH_DN_REF_FK;

drop index DJ_DJEH_REF_FK;

alter table DISTRIBUTIONJOBEXTERNALHOST drop constraint PK_DISTRIBUTIONJOBEXTERNALHOST;

quit;

