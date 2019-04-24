/* ********************************************************** */
/* name: PROD_FTNM_FLAG.sql                                   */
/* revised: 20171019 jr, creation                             */
/*          20171101 teh, moved from patches to add_ons       */
/* ********************************************************** */

/* For consistencey, this should be applied to the filemetadata table AFTER CREATE_FM_UNIQUE.
 * initializeSchema.pl will do this automatically, simply because this file is listed after CREATE_FM_UNIQUE.sql in its foreach loop*/
ALTER TABLE PRODUCTDESCRIPTION
ADD productdistflag VARCHAR(8);
exit;
