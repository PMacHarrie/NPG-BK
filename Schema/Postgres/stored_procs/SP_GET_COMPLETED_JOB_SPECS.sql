/* ********************************************************************** */
/* name: SP_GET_COMPLETED_JOB_SPECS.sql                                   */
/* purpose: Determines whether any existing Job Specs can be              */
/*          completed, sets their status to  "COMPLETE", and              */
/*          returns their ids in a list (refcursor), PGS app              */
/*          will create the PRODUCTIONJOB rows.                           */
/* revised: 20090626 lhf, create                                          */
/*          20090706 lhf, added jsi and jsp rows...                       */
/*          20090723 removed productInputDuration                         */
/*          20090723 remove F_GET_SECS                                    */
/*          20090819 lhf proto4 updates (prinputproduct table)            */
/*          20090903 lhf updated UNION portion to cover jobs              */
/*                   that have timed out (SYSDATE > JSITIMEOUTITME        */
/*          20100120 lhf Trigger offsets                                  */
/*          20100525 lhf/tjf correct orbital w/UNION                      */
/*          20100525 lhf/tjf add PRODUCTCOVERAGEGAPINTERVAL_DS            */
/*          20110503 lhf, add lock table for clustered factories          */
/*          20110705 lhf remove nowait from locktable (NDE-51)            */
/*          20111005 lhf/htp add granule pris offset handling             */
/*          20120120 pgm/ added fileDeletedFlag='N'                       */
/*          20120307 pgm/ added pjsStartTime > interval                   */
/*          20121026 lhf resourceid fix                                   */
/*          20121113 lhf NDE-680, add GranuleExact                        */
/*          20121210 lhf, replace resourceid fix w/joined ri/rp/rs        */
/*          20180109 jrh, ENTR-3959, do not double-count overlapping file */
/*                        observation times by using the new function     */
/*                        FN_GET_TOTAL_FILE_ACCUMULATION                  */
/* ********************************************************************** */

create or replace function SP_GET_COMPLETED_JOB_SPECS(
        v_hostname		in	varchar,
        v_servicename		in	varchar
)
returns refcursor
as $$

DECLARE

v_prodpartialjobids    refcursor;
v_prodpartialjobid     bigint;
v_pjscompleteresourceid bigint := 0;
v_prrulename varchar(255);
v_mysessionid bigint;
c_prodpartialjobids  refcursor;

BEGIN

  lock table PRODUCTIONJOBSPECLOCK in exclusive mode;

  /* ********************************* */
  /* # Populate the refcursor for output */
  /* # Removed pgm 08/06/13 Just select updated rows at the end */
  /* ********************************* */

  /* ******************************************************** */
  /* Populate the refcursor again and then perform the update */
  /* ******************************************************** */
  /*  open v_prodpartialjobids FOR */

select pg_backend_pid() into v_mysessionId ;
delete from jobTemp where sessionId = v_mysessionId;

insert into jobTemp
  select
	v_mysessionid,
      t2.PRODPARTIALJOBID as prodpartialjobid
  from (select PRODPARTIALJOBID, PRID, count(PRODUCTID) as prisCount1 from (select
          s.PRODPARTIALJOBID,
          i.PRID,
          p.PRODUCTID,
          r.PRPRODUCTCOVERAGEINTERVAL_DS
        from
          PRODUCTIONJOBSPEC s, PRODUCTIONRULE r, PRINPUTSPEC i, PRINPUTPRODUCT p, productDescription d
        where
          s.PJSCOMPLETIONSTATUS='INCOMPLETE' and
          s.PJSSTARTTIME >= now() - interval '32' day and
          s.PRID=r.PRID and
          i.PRID=r.PRID and
          p.PRISID=i.PRISID and
          p.PRINPUTPREFERENCE=1 and
          i.PRISNEED in ('REQUIRED','TRIGGER') and
          r.PRRULETYPE in ('Granule','Temporal','GranuleExact') and
          p.PRODUCTID=d.PRODUCTID and
          ((r.PRPRODUCTCOVERAGEINTERVAL_DS is null and FN_GET_TOTAL_FILE_ACCUMULATION(p.PRODUCTID, s.PJSOBSSTARTTIME + i.PRISLEFTOFFSETINTERVAL, s.PJSOBSENDTIME + i.PRISRIGHTOFFSETINTERVAL) >= FN_GET_MILLIS(s.PJSOBSENDTIME - s.PJSOBSSTARTTIME - i.PRISLEFTOFFSETINTERVAL + i.PRISRIGHTOFFSETINTERVAL))
          or (r.PRPRODUCTCOVERAGEINTERVAL_DS is not null and FN_GET_TOTAL_FILE_ACCUMULATION(p.PRODUCTID, s.PJSOBSSTARTTIME + i.PRISLEFTOFFSETINTERVAL, s.PJSOBSENDTIME + i.PRISRIGHTOFFSETINTERVAL) >= FN_GET_MILLIS(r.PRPRODUCTCOVERAGEINTERVAL_DS - i.PRISLEFTOFFSETINTERVAL + i.PRISRIGHTOFFSETINTERVAL)
          ))
        ) t1
         group by PRODPARTIALJOBID, PRID) t2,
         (select r.PRID, count(p.PRODUCTID) as prisCount2
          from PRODUCTIONRULE r, PRINPUTSPEC i,PRINPUTPRODUCT p
          where r.PRID = i.PRID and i.PRISID=p.PRISID and PRINPUTPREFERENCE=1 and i.PRISNEED in ('REQUIRED','TRIGGER')
          group by r.PRID) p1
  where
    t2.PRID = p1.PRID
    and prisCount1 = prisCount2

  /* UNION in orbital completion stuff */
  UNION
  select
	v_mysessionid,
      t4.PRODPARTIALJOBID as prodpartialjobid
  from (select PRODPARTIALJOBID, PRID, count(PRODUCTID) as prisCount1 from (select
          s.PRODPARTIALJOBID,
          i.PRID,
          p.PRODUCTID,
          PJSOBSENDTIME - PJSOBSSTARTTIME
        from
          PRODUCTIONJOBSPEC s, PRODUCTIONRULE r, PRINPUTSPEC i, PRINPUTPRODUCT p, productDescription d
        where
          s.PJSCOMPLETIONSTATUS='INCOMPLETE' and
          s.PJSSTARTTIME >= now() - interval '32' day and
          s.PRID=r.PRID and
          i.PRID=r.PRID and
          p.PRISID=i.PRISID and
          p.PRINPUTPREFERENCE=1 and
          i.PRISNEED in ('REQUIRED','TRIGGER') and
          r.PRRULETYPE = 'Orbital' and
          p.PRODUCTID=d.PRODUCTID and
          FN_GET_TOTAL_FILE_ACCUMULATION(p.PRODUCTID, s.PJSOBSSTARTTIME + i.PRISLEFTOFFSETINTERVAL, s.PJSOBSENDTIME + i.PRISRIGHTOFFSETINTERVAL) >= FN_GET_MILLIS(PJSOBSENDTIME - PJSOBSSTARTTIME)
        ) t3
         group by PRODPARTIALJOBID, PRID) t4,
         (select r.PRID, count(p.PRODUCTID) as prisCount2
          from PRODUCTIONRULE r, PRINPUTSPEC i,PRINPUTPRODUCT p
          where r.PRID = i.PRID and i.PRISID=p.PRISID and PRINPUTPREFERENCE=1 and i.PRISNEED in ('REQUIRED','TRIGGER')
          group by r.PRID) p2
  where
    t4.PRID = p2.PRID
    and prisCount1 = prisCount2

  /* UNION in those where the timeout has been reached */
  UNION
  select
	v_mysessionid,
      PRODPARTIALJOBID as prodpartialjobid
  from
      PRODUCTIONJOBSPEC pjs,PRODUCTIONRULE pr
  where
      pjs.PRID=pr.PRID and
      PJSCOMPLETIONSTATUS='INCOMPLETE' and
      now() > PJSTIMEOUTTIME;

open v_prodpartialjobids FOR
	select
		updatePrimaryKey as prodPartialJobId
	from
		jobTemp
	where
		sessionId = v_mysessionId;


  LOOP
    FETCH v_prodpartialjobids into v_prodpartialjobid;
    EXIT WHEN NOT FOUND;

    /* do all the jobparameters */
    insert into JOBSPECPARAMETERS
	select
		nextval('S_JOBSPECPARAMETERS'),
		v_prodpartialjobid,
		PRPARAMETERSEQNO,
		PRPARAMETERVALUE
	from
		PRPARAMETER p, productionJobSpec s
	where
		s.prodPartialJobId = v_prodpartialjobid
		and s.PRID = p.prId
	;

    update PRODUCTIONJOBSPEC set PJSCOMPLETIONSTATUS='COMPLETING', PJSCOMPLETERESOURCEID = v_pjscompleteresourceid
     where PRODPARTIALJOBID = v_prodpartialjobid;

  END LOOP;

  OPEN c_prodpartialjobids FOR
  select
      updatePrimaryKey as prodPartialJobId,
      PRRULENAME
  from jobTemp t, productionRule r, productionJobSpec s
  where sessionId = v_mysessionid and t.updatePrimaryKey = s.prodPartialJobId and s.prId = r.prId;

  return c_prodpartialjobids;

END;
$$ LANGUAGE plpgsql;
