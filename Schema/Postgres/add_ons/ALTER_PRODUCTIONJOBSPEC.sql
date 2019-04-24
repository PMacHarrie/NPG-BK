/* ************************************************************ */
/* name: ALTER_PRODUCTIONJOBSPEC.sql                            */
/* revised: 20171212 pgm/teh, creation                              */
/*   Removing an index and a constraint to improve              */
/*    SP_REQUESTJOB performance  (pgm dev, teh CM)              */
/*                                                              */
/* ************************************************************ */

alter table productionjobspec drop constraint FK_PRODUCTI_PRODJOBSI_JOBCLASS;
drop index PRODJOBSIZE_FK;

quit;

