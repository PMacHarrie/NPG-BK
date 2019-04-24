/* ********************************************************************** */
/* name: SP_GET_POTENTIAL_JOB_SPECS.sql                                   */
/* notes: POTENTIAL_JOB_SPECS table is a temporary solution               */
/* 20090709 commenting out productcoverageseconds, adding case            */
/*          to runintervalseconds                                         */
/* revised: 20090720 lhf/tjf, direct insert, no PJS table                 */
/*          20090721 lhf/tjf, adjust jobEnd to include last job           */
/*          20090813 lhf/tjf, added spatial qualification                 */
/*          20090814 lhf, completed spatial, set timeouttime              */
/*          20100215 lhf/tjf, add orbital type in                         */
/*          20100525 lhf/tjf, remove PJS from orbital froms               */
/*          20110201 lhf, use global temporary table                      */
/*          20110517 lhf, remove global temporary table, add              */
/*                        CREATING status                                 */
/*          20120523 lhf, NDE-607, update for build 4/5                   */
/*          20120530 lhf, cleanup                                         */
/*          20121025 lhf, addition of pjscreateresourceid on insert       */
/*          20121113 lhf, added lock table                                */
/*          20121210 lhf, replace resourceid fix w/joined ri/rp/rs        */
/*          20130101 dcp, updated pjstimeout to key off of local time     */
/*          20140902 teh, use new prActiveFlag_NSOF/prActiveFlag_CBU      */
/*          20150824 teh, ENTR-2043.  Add ProductionJobSpecCRLOCK table.  */
/*          20190107 jrh, Fixed computation of 'jobStart', 'jobEnd', and  */
/*                        'numJobs' for Temporal production rules with a  */
/*                        non-null prRunInterval_DS.                      */
/* ********************************************************************** */

create or replace function sp_get_potential_job_specs(
        v_fileid                in      BIGINT,
        v_productid             in      BIGINT,
        v_hostname              in      varchar
)
returns refcursor
as $$

#variable_conflict use_column -- resolve postgresql naming conflict

DECLARE

c_prIds refcursor;
v_prIds refcursor;
v_prid bigint;
v_jobstart timestamp;
v_jobend timestamp;
v_jobclass bigint;
v_jobpriority bigint;
v_numjobs bigint;
v_fst timestamp;
v_fet timestamp;
v_count bigint;
v_prproductcoverageinterval_ds INTERVAL DAY TO SECOND(5);
v_prruninterval_ds INTERVAL DAY TO SECOND(5);
v_prwaitforinputinterval_ds INTERVAL DAY to SECOND(5);
v_newjobendtime timestamp;
v_newjobstarttime timestamp;
v_pjstimeouttime timestamp;
v_pjscount bigint;
v_pjscreateresourceid bigint;
v_locationiscbu varchar(15);
v_mysessionId bigint;
v_numRules bigint;
v_fm_fileSpatialArea geography;
v_fm_fileStartTime timestamp;
v_fm_fileEndTime timestamp;

BEGIN
  lock table PRODUCTIONJOBSPECCRLOCK in exclusive mode;

  select CFGPARAMETERVALUE into v_locationiscbu
  from CONFIGURATIONREGISTRY
  where CFGPARAMETERNAME = 'LocationIsCBU';

   v_pjscreateresourceid := 0;

  select pg_backend_pid() into v_mysessionId;
  RAISE NOTICE 'Start Cursor  Get Potential(mysessionId %)', v_mysessionId; 
  delete from potJobTemp where sessionid = v_mysessionid;
  delete from potjobTemp2 where sessionid = v_mysessionid;
  delete from JobTemp where sessionid = v_mysessionid;

  select fileSpatialArea, fileStartTime, fileEndTime into v_fm_fileSpatialArea, v_fm_fileStartTime, v_fm_fileEndTime from fileMetadata where fileId = v_fileid;

  v_numRules := 0;
  select
        count(*) into v_numRules
  from
        productionRule r,
        prInputSpec s,
        prInputProduct p
  where
        p.productId = v_productid
        and p.prisid = s.prisid
        and r.prid = s.prid
        and prRuleType <> 'Orbital'
        and prisNeed = 'TRIGGER'
        and gzId is null
        and ((v_locationiscbu!='1' and PRACTIVEFLAG_NSOF=1) or (v_locationiscbu='1' and PRACTIVEFLAG_CBU=1));

  IF (v_numRules > 0)
  THEN
  insert into potJobTemp
    select v_mysessionId,
    PRID as PRID,
    jobStart as JOBSTART,
    jobEnd as JOBEND,
    JOBCLASS as JOBCLASS,
    JOBPRIORITY as JOBPRIORITY,
    PPCI as PPCI,
    PRI as PRI,
  (EXTRACT(DAY FROM(jobEnd-jobStart) )*86400 +
   EXTRACT(HOUR FROM(jobEnd-jobStart) )*3600 +
   EXTRACT(MINUTE FROM(jobEnd-jobStart) )*60 +
   EXTRACT(SECOND FROM(jobEnd-jobStart)) )/productCoverageSeconds as NUMJOBS,
   v_fm_fileStartTime as FST,
   v_fm_fileEndTime as FET,
   PWFII as PWFII
  from ( select v_fileid,
           pr.PRID,
           v_fm_fileStartTime,
           v_fm_fileEndTime,
           pr.PRPRODUCTCOVERAGEINTERVAL_DS as PPCI,
           pr.PRRUNINTERVAL_DS as PRI,
           pr.PRWAITFORINPUTINTERVAL_DS as PWFII,
           pr.PRRUNINTERVAL_DS, pr.PRSTARTBOUNDARYTIME,
           case
             when PRSTARTBOUNDARYTIME is not null
                  and PRPRODUCTCOVERAGEINTERVAL_DS is not null
                  and PRRUNINTERVAL_DS is null then
               PRSTARTBOUNDARYTIME + (PRPRODUCTCOVERAGEINTERVAL_DS *
               trunc((
                EXTRACT(DAY FROM (v_fm_fileStartTime-PRSTARTBOUNDARYTIME) )*86400 +
                EXTRACT(HOUR FROM (v_fm_fileStartTime-PRSTARTBOUNDARYTIME) )*3600 +
                EXTRACT(MINUTE FROM (v_fm_fileStartTime-PRSTARTBOUNDARYTIME) )*60 +
                EXTRACT(SECOND FROM (v_fm_fileStartTime-PRSTARTBOUNDARYTIME) )
               ) /
               (EXTRACT(DAY FROM(PRPRODUCTCOVERAGEINTERVAL_DS) )*86400 +
                EXTRACT(HOUR FROM(PRPRODUCTCOVERAGEINTERVAL_DS) )*3600 +
                EXTRACT(MINUTE FROM(PRPRODUCTCOVERAGEINTERVAL_DS) )*60 +
                EXTRACT(SECOND FROM(PRPRODUCTCOVERAGEINTERVAL_DS) ))
               ))
             when PRSTARTBOUNDARYTIME is not null
                  and PRPRODUCTCOVERAGEINTERVAL_DS is not null
                  and PRRUNINTERVAL_DS is not null then
               PRSTARTBOUNDARYTIME + (PRRUNINTERVAL_DS *
               trunc((
                EXTRACT(DAY FROM (v_fm_fileStartTime - PRPRODUCTCOVERAGEINTERVAL_DS - PRSTARTBOUNDARYTIME) )*86400 +
                EXTRACT(HOUR FROM (v_fm_fileStartTime - PRPRODUCTCOVERAGEINTERVAL_DS - PRSTARTBOUNDARYTIME) )*3600 +
                EXTRACT(MINUTE FROM (v_fm_fileStartTime - PRPRODUCTCOVERAGEINTERVAL_DS - PRSTARTBOUNDARYTIME) )*60 +
                EXTRACT(SECOND FROM (v_fm_fileStartTime - PRPRODUCTCOVERAGEINTERVAL_DS - PRSTARTBOUNDARYTIME) )
               ) /
               (EXTRACT(Day FROM(PRRUNINTERVAL_DS) )*86400 +
                EXTRACT(HOUR FROM(PRRUNINTERVAL_DS) )*3600 +
                EXTRACT(MINUTE FROM(PRRUNINTERVAL_DS) )*60 +
                EXTRACT(SECOND FROM(PRRUNINTERVAL_DS) ))
               +1))
             else
               v_fm_fileStartTime
           end as jobStart,
           case
             when PRPRODUCTCOVERAGEINTERVAL_DS is null then
               v_fm_fileEndTime
             when PRSTARTBOUNDARYTIME is null and PRRUNINTERVAL_DS is null then
               v_fm_fileStartTime + (PRPRODUCTCOVERAGEINTERVAL_DS * trunc((
                EXTRACT(DAY FROM (v_fm_fileEndTime-v_fm_fileStartTime) )*86400 +
                EXTRACT(HOUR FROM (v_fm_fileEndTime-v_fm_fileStartTime) )*3600 +
                EXTRACT(MINUTE FROM (v_fm_fileEndTime-v_fm_fileStartTime) )*60 +
                EXTRACT(SECOND FROM (v_fm_fileEndTime-v_fm_fileStartTime) )
                ) /
               (EXTRACT(Day FROM(PRPRODUCTCOVERAGEINTERVAL_DS) )*86400 +
                 EXTRACT(HOUR FROM(PRPRODUCTCOVERAGEINTERVAL_DS) )*3600 +
                 EXTRACT(MINUTE FROM(PRPRODUCTCOVERAGEINTERVAL_DS) )*60 +
                 EXTRACT(SECOND FROM(PRPRODUCTCOVERAGEINTERVAL_DS) ))
               +1))
             when PRRUNINTERVAL_DS is null and PRSTARTBOUNDARYTIME is not null then
               PRSTARTBOUNDARYTIME + (PRPRODUCTCOVERAGEINTERVAL_DS * trunc((
                EXTRACT(DAY FROM (v_fm_fileEndTime-PRSTARTBOUNDARYTIME) )*86400 +
                EXTRACT(HOUR FROM (v_fm_fileEndTime-PRSTARTBOUNDARYTIME) )*3600 +
                EXTRACT(MINUTE FROM (v_fm_fileEndTime-PRSTARTBOUNDARYTIME) )*60 +
                EXTRACT(SECOND FROM (v_fm_fileEndTime-PRSTARTBOUNDARYTIME) )
               ) /
               (EXTRACT(Day FROM(PRPRODUCTCOVERAGEINTERVAL_DS) )*86400 +
                 EXTRACT(HOUR FROM(PRPRODUCTCOVERAGEINTERVAL_DS) )*3600 +
                 EXTRACT(MINUTE FROM(PRPRODUCTCOVERAGEINTERVAL_DS) )*60 +
                 EXTRACT(SECOND FROM(PRPRODUCTCOVERAGEINTERVAL_DS) ))
               )) + PRPRODUCTCOVERAGEINTERVAL_DS
             /* All three are set */
             when PRSTARTBOUNDARYTIME is not null and PRRUNINTERVAL_DS is not null then
               PRSTARTBOUNDARYTIME + (PRRUNINTERVAL_DS * trunc((
                EXTRACT(DAY FROM (v_fm_fileEndTime-PRSTARTBOUNDARYTIME) )*86400 +
                EXTRACT(HOUR FROM (v_fm_fileEndTime-PRSTARTBOUNDARYTIME) )*3600 +
                EXTRACT(MINUTE FROM (v_fm_fileEndTime-PRSTARTBOUNDARYTIME) )*60 +
                EXTRACT(SECOND FROM (v_fm_fileEndTime-PRSTARTBOUNDARYTIME) )
               ) /
               (EXTRACT(Day FROM(PRRUNINTERVAL_DS) )*86400 +
                 EXTRACT(HOUR FROM(PRRUNINTERVAL_DS) )*3600 +
                 EXTRACT(MINUTE FROM(PRRUNINTERVAL_DS) )*60 +
                 EXTRACT(SECOND FROM(PRRUNINTERVAL_DS) ))
               +1))
             /* Traps invalid combinations, this makes Numjobs=0 (no jobs get created) */
             else
               v_fm_fileStartTime
           end as jobEnd,
           case
             /* Granule */
             when PRPRODUCTCOVERAGEINTERVAL_DS is null then
                EXTRACT(DAY FROM (v_fm_fileEndTime-v_fm_fileStartTime) )*86400 +
                EXTRACT(HOUR FROM (v_fm_fileEndTime-v_fm_fileStartTime) )*3600 +
                EXTRACT(MINUTE FROM (v_fm_fileEndTime-v_fm_fileStartTime) )*60 +
                EXTRACT(SECOND FROM (v_fm_fileEndTime-v_fm_fileStartTime) )
             /* Temporal */
             when PRSTARTBOUNDARYTIME is not null and PRRUNINTERVAL_DS is not null
                  and PRPRODUCTCOVERAGEINTERVAL_DS is not null then
               (EXTRACT(Day FROM(PRRUNINTERVAL_DS) )*86400 +
                EXTRACT(HOUR FROM(PRRUNINTERVAL_DS) )*3600 +
                EXTRACT(Minute FROM(PRRUNINTERVAL_DS) )*60 +
                EXTRACT(SECOND FROM(PRRUNINTERVAL_DS)) )
             else
               (EXTRACT(Day FROM(PRPRODUCTCOVERAGEINTERVAL_DS) )*86400 +
                EXTRACT(HOUR FROM(PRPRODUCTCOVERAGEINTERVAL_DS) )*3600 +
                EXTRACT(Minute FROM(PRPRODUCTCOVERAGEINTERVAL_DS) )*60 +
                EXTRACT(SECOND FROM(PRPRODUCTCOVERAGEINTERVAL_DS)) )
             end as productCoverageSeconds,
            PRISNEED,JOBCLASS,JOBPRIORITY,pr.PRPRODUCTCOVERAGEINTERVAL_DS
         from
           PRODUCTIONRULE pr, PRINPUTSPEC pis, PRINPUTPRODUCT pip
         where
           pr.PRRULETYPE <> 'Orbital'
           and pr.PRID=pis.PRID
           and pis.PRISID=pip.PRISID
           and pip.PRODUCTID=v_productid
           and pis.PRISNEED='TRIGGER'
           and pr.GZID is null
           and ((v_locationiscbu!='1' and PRACTIVEFLAG_NSOF=1) or (v_locationiscbu='1' and PRACTIVEFLAG_CBU=1))
         ) t;
  END IF;

  v_numRules := 0;

  select
        count(*) into v_numRules
  from
        productionRule r,
        prInputSpec s,
        prInputProduct p
  where
        p.productId = v_productid
        and p.prisid = s.prisid
        and r.prid = s.prid
        and prRuleType <> 'Orbital'
        and prisNeed = 'TRIGGER'
        and gzId is not null
        and ((v_locationiscbu!='1' and PRACTIVEFLAG_NSOF=1) or (v_locationiscbu='1' and PRACTIVEFLAG_CBU=1));


  IF (v_numRules > 0)
  THEN
  insert into potJobTemp
  select
    v_mysessionId,
    PRID as PRID,
    jobStart as JOBSTART,
    jobEnd as JOBEND,
    JOBCLASS as JOBCLASS,
    JOBPRIORITY as JOBPRIORITY,
    PPCI as PPCI,
    PRI as PRI,
  (EXTRACT(DAY FROM(jobEnd-jobStart) )*86400 +
   EXTRACT(HOUR FROM(jobEnd-jobStart) )*3600 +
   EXTRACT(MINUTE FROM(jobEnd-jobStart) )*60 +
   EXTRACT(SECOND FROM(jobEnd-jobStart)) )/productCoverageSeconds as NUMJOBS,
   v_fm_fileStartTime as FST,
   v_fm_fileEndTime as FET,
   PWFII as PWFII
  from ( select
           v_fileid,
           pr.PRID,
           v_fm_fileStartTime,
           v_fm_fileEndTime,
           pr.PRPRODUCTCOVERAGEINTERVAL_DS as PPCI,
           pr.PRRUNINTERVAL_DS as PRI,
           pr.PRWAITFORINPUTINTERVAL_DS as PWFII,
           pr.PRRUNINTERVAL_DS, pr.PRSTARTBOUNDARYTIME,
           case
             when PRSTARTBOUNDARYTIME is not null
                  and PRPRODUCTCOVERAGEINTERVAL_DS is not null
                  and PRRUNINTERVAL_DS is null then
               PRSTARTBOUNDARYTIME + (PRPRODUCTCOVERAGEINTERVAL_DS *
               trunc((
                EXTRACT(DAY FROM (v_fm_fileStartTime-PRSTARTBOUNDARYTIME) )*86400 +
                EXTRACT(HOUR FROM (v_fm_fileStartTime-PRSTARTBOUNDARYTIME) )*3600 +
                EXTRACT(MINUTE FROM (v_fm_fileStartTime-PRSTARTBOUNDARYTIME) )*60 +
                EXTRACT(SECOND FROM (v_fm_fileStartTime-PRSTARTBOUNDARYTIME) )
               ) /
               (EXTRACT(DAY FROM(PRPRODUCTCOVERAGEINTERVAL_DS) )*86400 +
                EXTRACT(HOUR FROM(PRPRODUCTCOVERAGEINTERVAL_DS) )*3600 +
                EXTRACT(MINUTE FROM(PRPRODUCTCOVERAGEINTERVAL_DS) )*60 +
                EXTRACT(SECOND FROM(PRPRODUCTCOVERAGEINTERVAL_DS) ))
               ))
             when PRSTARTBOUNDARYTIME is not null
                  and PRPRODUCTCOVERAGEINTERVAL_DS is not null
                  and PRRUNINTERVAL_DS is not null then
               PRSTARTBOUNDARYTIME + (PRRUNINTERVAL_DS *
               trunc((
                EXTRACT(DAY FROM (v_fm_fileStartTime - PRPRODUCTCOVERAGEINTERVAL_DS - PRSTARTBOUNDARYTIME) )*86400 +
                EXTRACT(HOUR FROM (v_fm_fileStartTime - PRPRODUCTCOVERAGEINTERVAL_DS - PRSTARTBOUNDARYTIME) )*3600 +
                EXTRACT(MINUTE FROM (v_fm_fileStartTime - PRPRODUCTCOVERAGEINTERVAL_DS - PRSTARTBOUNDARYTIME) )*60 +
                EXTRACT(SECOND FROM (v_fm_fileStartTime - PRPRODUCTCOVERAGEINTERVAL_DS - PRSTARTBOUNDARYTIME) )
               ) /
               (EXTRACT(Day FROM(PRRUNINTERVAL_DS) )*86400 +
                EXTRACT(HOUR FROM(PRRUNINTERVAL_DS) )*3600 +
                EXTRACT(MINUTE FROM(PRRUNINTERVAL_DS) )*60 +
                EXTRACT(SECOND FROM(PRRUNINTERVAL_DS) ))
               +1))
             else
               v_fm_fileStartTime
           end as jobStart,
           case
             when PRPRODUCTCOVERAGEINTERVAL_DS is null then
               v_fm_fileEndTime
             when PRSTARTBOUNDARYTIME is null and PRRUNINTERVAL_DS is null then
               v_fm_fileStartTime + (PRPRODUCTCOVERAGEINTERVAL_DS * trunc((
                EXTRACT(DAY FROM (v_fm_fileEndTime-v_fm_fileStartTime) )*86400 +
                EXTRACT(HOUR FROM (v_fm_fileEndTime-v_fm_fileStartTime) )*3600 +
                EXTRACT(MINUTE FROM (v_fm_fileEndTime-v_fm_fileStartTime) )*60 +
                EXTRACT(SECOND FROM (v_fm_fileEndTime-v_fm_fileStartTime) )
                ) /
               (EXTRACT(Day FROM(PRPRODUCTCOVERAGEINTERVAL_DS) )*86400 +
                 EXTRACT(HOUR FROM(PRPRODUCTCOVERAGEINTERVAL_DS) )*3600 +
                 EXTRACT(MINUTE FROM(PRPRODUCTCOVERAGEINTERVAL_DS) )*60 +
                 EXTRACT(SECOND FROM(PRPRODUCTCOVERAGEINTERVAL_DS) ))
               +1))
             when PRRUNINTERVAL_DS is null and PRSTARTBOUNDARYTIME is not null then
               PRSTARTBOUNDARYTIME + (PRPRODUCTCOVERAGEINTERVAL_DS * trunc((
                EXTRACT(DAY FROM (v_fm_fileEndTime-PRSTARTBOUNDARYTIME) )*86400 +
                EXTRACT(HOUR FROM (v_fm_fileEndTime-PRSTARTBOUNDARYTIME) )*3600 +
                EXTRACT(MINUTE FROM (v_fm_fileEndTime-PRSTARTBOUNDARYTIME) )*60 +
                EXTRACT(SECOND FROM (v_fm_fileEndTime-PRSTARTBOUNDARYTIME) )
               ) /
               (EXTRACT(Day FROM(PRPRODUCTCOVERAGEINTERVAL_DS) )*86400 +
                 EXTRACT(HOUR FROM(PRPRODUCTCOVERAGEINTERVAL_DS) )*3600 +
                 EXTRACT(MINUTE FROM(PRPRODUCTCOVERAGEINTERVAL_DS) )*60 +
                 EXTRACT(SECOND FROM(PRPRODUCTCOVERAGEINTERVAL_DS) ))
               )) + PRPRODUCTCOVERAGEINTERVAL_DS
             /* All three are set */
             when PRSTARTBOUNDARYTIME is not null and PRRUNINTERVAL_DS is not null then
               PRSTARTBOUNDARYTIME + (PRRUNINTERVAL_DS * trunc((
                EXTRACT(DAY FROM (v_fm_fileEndTime-PRSTARTBOUNDARYTIME) )*86400 +
                EXTRACT(HOUR FROM (v_fm_fileEndTime-PRSTARTBOUNDARYTIME) )*3600 +
                EXTRACT(MINUTE FROM (v_fm_fileEndTime-PRSTARTBOUNDARYTIME) )*60 +
                EXTRACT(SECOND FROM (v_fm_fileEndTime-PRSTARTBOUNDARYTIME) )
               ) /
               (EXTRACT(Day FROM(PRRUNINTERVAL_DS) )*86400 +
                 EXTRACT(HOUR FROM(PRRUNINTERVAL_DS) )*3600 +
                 EXTRACT(MINUTE FROM(PRRUNINTERVAL_DS) )*60 +
                 EXTRACT(SECOND FROM(PRRUNINTERVAL_DS) ))
               +1))
             /* Traps invalid combinations, this makes Numjobs=0 (no jobs get created) */
             else
               v_fm_fileStartTime
           end as jobEnd,
           case
             /* Granule */
             when PRPRODUCTCOVERAGEINTERVAL_DS is null then
                EXTRACT(DAY FROM (v_fm_fileEndTime-v_fm_fileStartTime) )*86400 +
                EXTRACT(HOUR FROM (v_fm_fileEndTime-v_fm_fileStartTime) )*3600 +
                EXTRACT(MINUTE FROM (v_fm_fileEndTime-v_fm_fileStartTime) )*60 +
                EXTRACT(SECOND FROM (v_fm_fileEndTime-v_fm_fileStartTime) )
             /* Temporal */
             when PRSTARTBOUNDARYTIME is not null and PRRUNINTERVAL_DS is not null
                  and PRPRODUCTCOVERAGEINTERVAL_DS is not null then
               (EXTRACT(Day FROM(PRRUNINTERVAL_DS) )*86400 +
                EXTRACT(HOUR FROM(PRRUNINTERVAL_DS) )*3600 +
                EXTRACT(Minute FROM(PRRUNINTERVAL_DS) )*60 +
                EXTRACT(SECOND FROM(PRRUNINTERVAL_DS)) )
             else
               (EXTRACT(Day FROM(PRPRODUCTCOVERAGEINTERVAL_DS) )*86400 +
                EXTRACT(HOUR FROM(PRPRODUCTCOVERAGEINTERVAL_DS) )*3600 +
                EXTRACT(Minute FROM(PRPRODUCTCOVERAGEINTERVAL_DS) )*60 +
                EXTRACT(SECOND FROM(PRPRODUCTCOVERAGEINTERVAL_DS)) )
             end as productCoverageSeconds,
            PRISNEED,JOBCLASS,JOBPRIORITY,pr.PRPRODUCTCOVERAGEINTERVAL_DS
         from
           PRODUCTIONRULE pr, PRINPUTSPEC pis, PRINPUTPRODUCT pip, GAZETTEER gz
         where
           pr.PRRULETYPE <> 'Orbital'
           and pr.PRID=pis.PRID
           and pis.PRISID=pip.PRISID
           and pip.PRODUCTID=v_productid
           and pis.PRISNEED='TRIGGER'
           and pr.GZID is not null
	   and v_fm_fileSpatialArea is not null
           and pr.GZID=gz.GZID
           and ((v_locationiscbu!='1' and PRACTIVEFLAG_NSOF=1) or (v_locationiscbu='1' and PRACTIVEFLAG_CBU=1))
           and ST_Intersects(gz.GZLOCATIONSPATIAL,v_fm_fileSpatialArea) ) t ;
  END IF;

  v_numRules := 0;

  select
        count(*) into v_numRules
  from
        productionRule r,
        prInputSpec s,
        prInputProduct p
  where
        p.productId = v_productid
        and p.prisid = s.prisid
        and r.prid = s.prid
        and prRuleType = 'Orbital'
        and prisNeed = 'TRIGGER'
        and gzId is null
        and ((v_locationiscbu!='1' and PRACTIVEFLAG_NSOF=1) or (v_locationiscbu='1' and PRACTIVEFLAG_CBU=1));


  IF (v_numRules > 0)
  THEN
  insert into potJobTemp
select
    v_mysessionId,
    pr.PRID as PRID,
    po.PLORBITSTARTTIME as JOBSTART,
    po.PLORBITENDTIME as JOBEND,
    pr.JOBCLASS as JOBCLASS,
    pr.JOBPRIORITY as JOBPRIORITY,
    pr.PRPRODUCTCOVERAGEINTERVAL_DS as PPCI,
    pr.PRRUNINTERVAL_DS as PRI,
    1 as NUMJOBS,
    v_fm_fileStartTime as FST,
    v_fm_fileEndTime as FET,
    pr.PRWAITFORINPUTINTERVAL_DS as PWFII
from
    PRODUCTIONRULE pr, PRINPUTSPEC pis, PRINPUTPRODUCT pip,
    PLATFORM pf, PRODUCTPLATFORM_XREF ppx, PLATFORMORBIT po
where
    pr.PRRULETYPE = 'Orbital'
    and pr.PRID=pis.PRID
    and pis.PRISID=pip.PRISID
    and pip.PRODUCTID=v_productId
    and pf.PLATFORMID=ppx.PLATFORMID
    and ppx.PRODUCTID=v_productId
    and pr.PLORBITTYPEID=po.PLORBITTYPEID
    and v_fm_fileEndTime > po.PLORBITSTARTTIME and v_fm_fileStartTime < po.PLORBITENDTIME
    and pis.PRISNEED='TRIGGER'
    and ((v_locationiscbu!='1' and PRACTIVEFLAG_NSOF=1) or (v_locationiscbu='1' and PRACTIVEFLAG_CBU=1))
    and pr.GZID is null;
  END IF;

  v_numRules := 0;

  select
        count(*) into v_numRules
  from
        productionRule r,
        prInputSpec s,
        prInputProduct p
  where
        p.productId = v_productid
        and p.prisid = s.prisid
        and r.prid = s.prid
        and prRuleType = 'Orbital'
        and prisNeed = 'TRIGGER'
        and gzId is not null
        and ((v_locationiscbu!='1' and PRACTIVEFLAG_NSOF=1) or (v_locationiscbu='1' and PRACTIVEFLAG_CBU=1));


  IF (v_numRules > 0)
  THEN
  insert into potJobTemp
select
    v_mysessionId,
    pr.PRID as PRID,
    po.PLORBITSTARTTIME as JOBSTART,
    po.PLORBITENDTIME as JOBEND,
    pr.JOBCLASS as JOBCLASS,
    pr.JOBPRIORITY as JOBPRIORITY,
    pr.PRPRODUCTCOVERAGEINTERVAL_DS as PPCI,
    pr.PRRUNINTERVAL_DS as PRI,
    1 as NUMJOBS,
    v_fm_fileStartTime as FST,
    v_fm_fileEndTime as FET,
    pr.PRWAITFORINPUTINTERVAL_DS as PWFII
from
    PRODUCTIONRULE pr, PRINPUTSPEC pis, PRINPUTPRODUCT pip,
    PLATFORM pf, PRODUCTPLATFORM_XREF ppx, PLATFORMORBIT po, GAZETTEER gz
where
    pr.PRRULETYPE = 'Orbital'
    and pr.PRID=pis.PRID
    and pis.PRISID=pip.PRISID
    and pip.PRODUCTID=v_productid
    and pf.PLATFORMID=ppx.PLATFORMID
    and ppx.PRODUCTID=pip.PRODUCTID
    and pr.PLORBITTYPEID=po.PLORBITTYPEID
    and v_fm_fileEndTime > po.PLORBITSTARTTIME and v_fm_fileStartTime < po.PLORBITENDTIME
    and pis.PRISNEED='TRIGGER'
    and pr.GZID is not null
    and pr.GZID=gz.GZID
    and v_fm_fileSpatialArea is not null
    and ((v_locationiscbu!='1' and PRACTIVEFLAG_NSOF=1) or (v_locationiscbu='1' and PRACTIVEFLAG_CBU=1))
    and ST_Intersects(gz.GZLOCATIONSPATIAL,v_fm_fileSpatialArea) ;
  END IF;

  OPEN v_prIds FOR select distinct sessionid, v_prid, v_jobstart, v_jobend, v_jobclass, v_jobpriority, v_prproductcoverageinterval_ds, v_prruninterval_ds, v_numjobs, v_fst, v_fet, v_prwaitforinputinterval_ds 
	from potjobtemp 
	where sessionid = v_mysessionId;

  RAISE NOTICE 'Before Loop Get Potential(v_mysessionId %, fileId %, productId %)', v_mysessionId, v_fileId, v_productId; 

  FETCH v_prIds into v_mysessionId, v_prid,v_jobstart,v_jobend,v_jobclass,v_jobpriority,v_prproductcoverageinterval_ds,v_prruninterval_ds,v_numjobs,v_fst,v_fet,v_prwaitforinputinterval_ds;
  RAISE NOTICE 'After 1st Fetch Get Potential(v_mysessionId %, fileId %, productId %)', v_mysessionId, v_fileId, v_productId; 

  WHILE FOUND LOOP

    /* If no run interval is set, then jobs are run each product coverage interval */
    IF v_prruninterval_ds is null THEN
      v_prruninterval_ds := v_prproductcoverageinterval_ds;
    END IF;

    v_count := 1;
    v_newjobstarttime := v_jobstart;

    /* For each job (in the production rule) */
    WHILE v_count <= v_numjobs LOOP

      /* if it's a temporal rule */
      IF v_prproductcoverageinterval_ds is not null THEN
        v_newjobendtime := v_newjobstarttime + v_prproductcoverageinterval_ds;
      ELSE
        /* it's granule rule or it's an orbital rule */
        v_newjobendtime := v_jobend;
      END IF;

      /* debug: DBMS_OUTPUT.put_line('Jobs returned: '||v_prid||'<==>'||v_newjobstarttime);*/
      v_pjscount := 0;
      select count(*) into v_pjscount from PRODUCTIONJOBSPEC where PRID = v_prid and
        PJSOBSSTARTTIME = v_newjobstarttime and PJSOBSENDTIME = v_newjobendtime;

      /* If pjs row does not exist (yet) and job boundarys aren't outside of file boundaries */
      IF v_pjscount = 0 THEN
        IF v_newjobendtime > v_fst AND v_newjobstarttime < v_fet THEN
          insert into PRODUCTIONJOBSPEC (PRODPARTIALJOBID,PRID,JOBCLASS,JOBPRIORITY,PJSOBSSTARTTIME,
           PJSOBSENDTIME,PJSSTARTTIME,PJSCOMPLETIONSTATUS,PJSTIMEOUTTIME, PJSCREATERESOURCEID) values (nextval('S_PRODUCTIONJOBSPEC'),v_prid,v_jobclass,
           v_jobpriority,v_newjobstarttime,v_newjobendtime,localtimestamp,'INCOMPLETE',localtimestamp+v_prwaitforinputinterval_ds,v_pjscreateresourceid);
           insert into potjobTemp2 values(v_mysessionId, currval('S_PRODUCTIONJOBSPEC'));
        END IF;
      END IF;
      v_newjobstarttime := v_newjobstarttime + v_prruninterval_ds;

      v_count := v_count + 1;

    END LOOP;
/*    RAISE NOTICE 'Before Loop Fetch Get Potential(v_mysessionId %, fileId %, productId %)', v_mysessionId, v_fileId, v_productId; */
    FETCH v_prIds into v_mysessionId, v_prid,v_jobstart,v_jobend,v_jobclass,v_jobpriority,v_prproductcoverageinterval_ds,v_prruninterval_ds,v_numjobs,v_fst,v_fet,v_prwaitforinputinterval_ds;
/*    RAISE NOTICE 'Interating Get Potential(v_mysessionId %, fileId %, productId %)', v_mysessionId, v_fileId, v_productId; */
  END LOOP;
/*    RAISE NOTICE 'Done Loop Get Potential(fileId %, productId %)', v_fileId, v_productId; */

 open c_prIds FOR
    select PRID as PRID,
      to_char(PJSOBSSTARTTIME, 'YYYY-MM-DD HH24:MI:SS') as JOBSTART,
      to_char(PJSOBSENDTIME, 'YYYY-MM-DD HH24:MI:SS') as JOBEND,
      JOBCLASS as JOBCLASS,
      JOBPRIORITY as JOBPRIORITY
    from PRODUCTIONJOBSPEC s, potjobTemp2 j
    where j.sessionId = v_mysessionId and j.updatePrimaryKey = s.prodPartialJobId;

/*    RAISE NOTICE 'Done Cursor  Get Potential(mysessionId %)', v_mysessionId; */
/*    RAISE NOTICE 'Done Cursor  Get Potential(fileId %, productId %)', v_fileId, v_productId; */

  RETURN c_prIds;
END;
$$ LANGUAGE plpgsql;
