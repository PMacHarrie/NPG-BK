/* ************************************************************ */
/* name: CREATE_FM_UNIQUE.sql                                   */
/* revised: 20170406 teh, creation (based heavily on pm's work) */
/* ************************************************************ */

drop index fm_prodEndTime;
alter table fileMetadata drop constraint fm_unique;
alter table fileMetadata drop column fileProductSupplementMetadata;
drop index fm_unique;
alter table fileMetadata add fileProductSupplementMetadata varchar(32) null;
--Populate fileProductSupplementMetadata so that extant ADECK/TC/GVF files don't prevent add constraint
   update filemetadata f set FILEPRODUCTSUPPLEMENTMETADATA =
     substr(f.filename,                        --substr(string,start position, length) so string is filename...
         --position = regex to get 1st number after '-subString', no matter number of digits, then add 1
         (select regexp_substr(IPSOPTIONALPARAMETERS, '\d+', instr(ipsoptionalparameters,'-subString')+ 11,1)
            from ingestprocessstep ips where ips.productid = f.productid and ipsoptionalparameters like '%-subString%') + 1, 
         --length = 2nd number - 1st:
         ((select regexp_substr(IPSOPTIONALPARAMETERS, '\d+', instr(ipsoptionalparameters,'-subString')+ 11,2)
            from ingestprocessstep ips where ips.productid = f.productid and ipsoptionalparameters like '%-subString%') - 
            (select regexp_substr(IPSOPTIONALPARAMETERS, '\d+', instr(ipsoptionalparameters,'-subString')+ 11,1)
            from ingestprocessstep ips where ips.productid = f.productid and ipsoptionalparameters like '%-subString%'))
         )
    where f.productid in 
   (select productid from ingestprocessstep where ipsoptionalparameters like '%-subString%');
--Now add constraint:
--Note that fileStartTime can be removed from the constraint once MIRS issue ENTR-3825 is resolved.
alter table fileMetadata add constraint fm_unique unique(productId, fileEndTime, fileProductSupplementMetadata, fileStartTime);
--select index_name from user_indexes where table_name = 'FILEMETADATA';
quit


--The following 2 queries will ID 'real' duplicates - and then delete them.
--Advise running these manually before hand, but only needed the first time adding this constraint.

--ID which files:
--  select productid, filestarttime, fileendtime, fileinserttime, filename, fileid
--  from (
--    select productid, fileid, fileendtime, fileinserttime, filename, filestarttime,
--      LAG(productid, 1, 0) over (order by productid, fileendtime) as prevProductid,
--      LAG(fileendtime, 1, null) over (order by productid, fileendtime) as prevfileendtime
--    from FILEMETADATA
--    where productid not in 
--       (select productid from ingestprocessstep where ipsoptionalparameters like '%-subString%')
--  )
--  where productid = prevProductid and fileendtime = prevfileendtime
--  order by productid, filestarttime, fileendtime, fileinserttime;

--Delete them: keeps oldest (most likely to be linked to jobs, etc)

--v_mysessionId number;
--BEGIN
--select distinct sid into v_mysessionId from v$mystat;
--delete JobTemp where sessionid = v_mysessionid;
--delete potjobtemp2 where sessionid = v_mysessionid;
----Save the fileids
--  insert into jobtemp (sessionid, UPDATEPRIMARYKEY) 
--  select (v_mysessionID, fileid) from 
--  (select fileid
--    from (
--    select productid, fileid, fileendtime, fileinserttime, filename, filestarttime,
--      LAG(productid, 1, 0) over (order by productid, fileendtime) as prevProductid,
--      LAG(fileendtime, 1, null) over (order by productid, fileendtime) as prevfileendtime
--    from FILEMETADATA
--    where productid not in 
--       (select productid from ingestprocessstep where ipsoptionalparameters like '%-subString%')
--    )
--   where productid = prevProductid and fileendtime = prevfileendtime
--  );
----Save the prodpartialjobids (if any)
-- insert into potjobtemp2 (sessionid, UPDATEPRIMARYKEY) 
--  select (v_mysessionID, prodpartialjobid) from
--  ( select prodpartialjobid from jobspecinput where fileid in (select UPDATEPRIMARYKEY from JobTemp where sessionid = v_mysessionid)
--  );  

----Delete things   
--  delete productionjoblogmessages where prjobid in 
--  (  select prjobid from productionjob where prodpartialjobid in 
--     (  select UPDATEPRIMARYKEY from potjobtemp2 where sessionid = v_mysessionid
--     )
--  );
--  delete productionjoboutputfiles where prjobid in 
--  (  select prjobid from productionjob where prodpartialjobid in 
--     (  select UPDATEPRIMARYKEY from potjobtemp2 where sessionid = v_mysessionid
--     )
--  );
--  delete productionjob where prodpartialjobid in 
--     (  select UPDATEPRIMARYKEY from potjobtemp2 where sessionid = v_mysessionid
--     );
--  delete jobspecoutput where prodpartialjobid in 
--     (  select UPDATEPRIMARYKEY from potjobtemp2 where sessionid = v_mysessionid
--     );
--  delete jobspecparameters where prodpartialjobid in 
--     (  select UPDATEPRIMARYKEY from potjobtemp2 where sessionid = v_mysessionid
--     );
--  delete jobspecinput where prodpartialjobid in 
--     (  select UPDATEPRIMARYKEY from potjobtemp2 where sessionid = v_mysessionid
--     );
--  delete productionjobspec where prodpartialjobid in 
--     (  select UPDATEPRIMARYKEY from potjobtemp2 where sessionid = v_mysessionid
--     );
--  delete filequalitysummary where fileid in 
--     (  select UPDATEPRIMARYKEY from jobtemp where sessionid = v_mysessionid
--     );
--  delete filemetadata where fileid in 
--     (  select UPDATEPRIMARYKEY from jobtemp where sessionid = v_mysessionid
--     );
--  delete JobTemp where sessionid = v_mysessionid;
--  delete potjobtemp2 where sessionid = v_mysessionid;
--commit;
--END
