

/* ************************************************************ */
/* name: SP_GET_FILE_ACCUMULATION.sql                           */
/* purpose: Returns top preference that meets the file          */ 
/*          accumulation threshold defined                      */
/* revisions:  tjf 20090828 creation                            */ 
/*             lhf 20110428 removal of REQUIREDPOINT join       */
/* ************************************************************ */

create or replace function SP_GET_FILE_ACCUMULATION(
        c_preference  out     refcursor,
        p_ppjid       in      bigint
)
as $$

BEGIN

  OPEN c_preference FOR
  select 
        pip.pripid,
        pis.PRISID,
        pip.PRINPUTPREFERENCE, 
        pis.PRISFILEACCUMULATIONTHRESHOLD,
        FN_GET_MILLIS((pjs.PJSOBSENDTIME+pis.PRISRIGHTOFFSETINTERVAL)-(pjs.PJSOBSSTARTTIME+pis.PRISLEFTOFFSETINTERVAL)) as JOBCOVERAGE
  from 	
        PRODUCTIONJOBSPEC pjs, 
        PRINPUTSPEC pis,
        PRINPUTPRODUCT pip,
        PRODUCTIONRULE pr
  where 
	pis.PRID=pr.PRID and pjs.PRID=pis.PRID and
	pip.PRISID=pis.PRISID and
	pjs.PRODPARTIALJOBID=p_ppjid and
        pis.PRISNEED in ('REQUIRED','TRIGGER') 
  order by
        prisid, prinputpreference;

END;
$$ LANGUAGE plpgsql;
