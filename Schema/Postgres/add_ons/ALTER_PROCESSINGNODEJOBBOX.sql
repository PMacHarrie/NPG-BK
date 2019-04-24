/* ************************************************************ */
/* name: ALTER_PROCESSINGNODEJOBBOX.sql                         */
/* revised: 20171110 teh, creation                              */
/*   Update to job priority meaning requires dropping this      */
/*    column.                                                   */
/*    This should be done for all NDE installs going forward.   */
/*                                                              */
/* ************************************************************ */

alter table processingnodejobbox drop column jobpriority;

quit;

