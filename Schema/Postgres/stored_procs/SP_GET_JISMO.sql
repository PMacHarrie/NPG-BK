/* ************************************************************ */
/* name: SP_GET_JISMO.sql                                       */
/* purpose: Returns rows that will become the Job Input Spec    */
/*          Message Object (jismo)                              */
/* revised: 20090821 lhf, create                                */
/*          20090824 lhf, add UNION and GZID null checking      */
/*          20090825 lhf, add PRISID, PRODUCTID, FAT            */
/*          20090902 tjf, add order by prisid, preference       */
/*          20100217 lhf, change to remove equal signs          */
/*          20120120 pgm, added range to end time for inside-inside */
/*          20121113 lhf, added GranuleExact NDE-561            */
/* notes: There are 3 portions UNIONed together, the first gets */
/*        inputs that satisfy any spatial specification, the    */
/*        second gets those without a spatial requirement, and  */
/*        the third is specifically for point data (like gfs)   */
/* ************************************************************ */

create or replace function SP_GET_JISMO(
        p_ppjid     in      bigint
)
returns refcursor
as $$
DECLARE

	c_jismo refcursor;

BEGIN

  OPEN c_jismo FOR

/* Inside-Inside */
  select
        fm.FILEID,
        pis.PRISID,
        pip.PRIPID,
        pip.PRODUCTID,
        pip.PRINPUTPREFERENCE,
        pjs.PJSOBSSTARTTIME,
        pjs.PJSOBSENDTIME,
        pjs.PJSTIMEOUTTIME,
        pis.PRISFILEHANDLE,
        pis.PRISTEST,
        FN_GET_MILLIS((fm.FILEENDTIME-fm.FILESTARTTIME)+PRODUCTCOVERAGEGAPINTERVAL_DS) as FILEJOBCOVERAGE
  from
        FILEMETADATA fm, PRODUCTIONRULE pr, PRODUCTIONJOBSPEC pjs,
        PRINPUTSPEC pis, PRINPUTPRODUCT pip, PRODUCTDESCRIPTION pd, GAZETTEER gaz
  where
        pr.PRRULETYPE <> 'GranuleExact' and
                pis.PRID = pr.PRID and pjs.PRID = pis.PRID and
        pip.PRISID = pis.PRISID and
        fm.PRODUCTID = pip.PRODUCTID and
        fm.PRODUCTID = pd.PRODUCTID and
        fm.FILESTARTTIME >= (pjs.PJSOBSSTARTTIME+pis.PRISLEFTOFFSETINTERVAL) and
        fm.FILEENDTIME between (pjs.PJSOBSSTARTTIME+pis.PRISLEFTOFFSETINTERVAL) and (pjs.PJSOBSENDTIME+pis.PRISRIGHTOFFSETINTERVAL) and
        fm.FILEDELETEDFLAG='N' and
        pr.GZID is not null and
        pr.GZID = gaz.GZID and
        ST_Intersects(gaz.GZLOCATIONSPATIAL,fm.FILESPATIALAREA) and
        pis.PRISNEED in ('REQUIRED','TRIGGER','OPTIONAL') and
        pjs.PRODPARTIALJOBID = p_ppjid
UNION
  select
        fm.FILEID,
        pip.PRISID,
        pip.PRIPID,
        pip.PRODUCTID,
        pip.PRINPUTPREFERENCE,
        pjs.PJSOBSSTARTTIME,
        pjs.PJSOBSENDTIME,
        pjs.PJSTIMEOUTTIME,
        pis.PRISFILEHANDLE,
        pis.PRISTEST,
        FN_GET_MILLIS((fm.FILEENDTIME-fm.FILESTARTTIME)+PRODUCTCOVERAGEGAPINTERVAL_DS) as FILEJOBCOVERAGE
  from
        FILEMETADATA fm, PRODUCTIONRULE pr, PRODUCTIONJOBSPEC pjs,
        PRINPUTSPEC pis, PRINPUTPRODUCT pip, PRODUCTDESCRIPTION pd
  where
        pr.PRRULETYPE <> 'GranuleExact' and
                pis.PRID = pr.PRID and pjs.PRID = pis.PRID and
        pip.PRISID = pis.PRISID and
        fm.PRODUCTID = pip.PRODUCTID and
        fm.PRODUCTID = pd.PRODUCTID and
        fm.FILESTARTTIME >= (pjs.PJSOBSSTARTTIME+pis.PRISLEFTOFFSETINTERVAL) and
        fm.FILEENDTIME between (pjs.PJSOBSSTARTTIME+pis.PRISLEFTOFFSETINTERVAL) and (pjs.PJSOBSENDTIME+pis.PRISRIGHTOFFSETINTERVAL) and
        fm.FILEDELETEDFLAG='N' and
        pr.GZID is null and
        pis.PRISNEED in ('REQUIRED','TRIGGER','OPTIONAL') and
        pjs.PRODPARTIALJOBID = p_ppjid
UNION

/* Before-Inside */
  select
        fm.FILEID,
        pis.PRISID,
        pip.PRIPID,
        pip.PRODUCTID,
        pip.PRINPUTPREFERENCE,
        pjs.PJSOBSSTARTTIME,
        pjs.PJSOBSENDTIME,
        pjs.PJSTIMEOUTTIME,
        pis.PRISFILEHANDLE,
        pis.PRISTEST,
        FN_GET_MILLIS((fm.FILEENDTIME-(pjs.PJSOBSSTARTTIME+pis.PRISLEFTOFFSETINTERVAL))+PRODUCTCOVERAGEGAPINTERVAL_DS) as FILEJOBCOVERAGE
  from
        FILEMETADATA fm, PRODUCTIONRULE pr, PRODUCTIONJOBSPEC pjs,
        PRINPUTSPEC pis, PRINPUTPRODUCT pip, PRODUCTDESCRIPTION pd, GAZETTEER gaz
  where
        pr.PRRULETYPE <> 'GranuleExact' and pr.PRRULETYPE <> 'Temporal' and
                pis.PRID = pr.PRID and pjs.PRID = pis.PRID and
        pip.PRISID = pis.PRISID and
        fm.PRODUCTID = pip.PRODUCTID and
        fm.PRODUCTID = pd.PRODUCTID and
        fm.FILESTARTTIME < (pjs.PJSOBSSTARTTIME+pis.PRISLEFTOFFSETINTERVAL) and
        fm.FILEENDTIME > (pjs.PJSOBSSTARTTIME+pis.PRISLEFTOFFSETINTERVAL) and
        fm.FILEENDTIME <= (pjs.PJSOBSENDTIME+pis.PRISRIGHTOFFSETINTERVAL) and
        fm.FILEDELETEDFLAG='N' and
        pr.GZID is not null and
        pr.GZID = gaz.GZID and
        ST_intersects(gaz.GZLOCATIONSPATIAL,fm.FILESPATIALAREA) and
        pis.PRISNEED in ('REQUIRED','TRIGGER','OPTIONAL') and
        pjs.PRODPARTIALJOBID = p_ppjid
UNION
  select
        fm.FILEID,
        pip.PRISID,
        pip.PRIPID,
        pip.PRODUCTID,
        pip.PRINPUTPREFERENCE,
        pjs.PJSOBSSTARTTIME,
        pjs.PJSOBSENDTIME,
        pjs.PJSTIMEOUTTIME,
        pis.PRISFILEHANDLE,
        pis.PRISTEST,
        FN_GET_MILLIS((fm.FILEENDTIME-(pjs.PJSOBSSTARTTIME+pis.PRISLEFTOFFSETINTERVAL))+PRODUCTCOVERAGEGAPINTERVAL_DS) as FILEJOBCOVERAGE
  from
        FILEMETADATA fm, PRODUCTIONRULE pr, PRODUCTIONJOBSPEC pjs,
        PRINPUTSPEC pis, PRINPUTPRODUCT pip, PRODUCTDESCRIPTION pd
  where
        pr.PRRULETYPE <> 'GranuleExact' and pr.PRRULETYPE <> 'Temporal' and
                pis.PRID = pr.PRID and pjs.PRID = pis.PRID and
        pip.PRISID = pis.PRISID and
        fm.PRODUCTID = pip.PRODUCTID and
        fm.PRODUCTID = pd.PRODUCTID and
        fm.FILESTARTTIME < (pjs.PJSOBSSTARTTIME+pis.PRISLEFTOFFSETINTERVAL) and
        fm.FILEENDTIME > (pjs.PJSOBSSTARTTIME+pis.PRISLEFTOFFSETINTERVAL) and
        fm.FILEENDTIME <= (pjs.PJSOBSENDTIME+pis.PRISRIGHTOFFSETINTERVAL) and
        fm.FILEDELETEDFLAG='N' and
        pr.GZID is null and
        pis.PRISNEED in ('REQUIRED','TRIGGER','OPTIONAL') and
        pjs.PRODPARTIALJOBID = p_ppjid
UNION 

/* Inside-After */
  select
        fm.FILEID,
        pis.PRISID,
        pip.PRIPID,
        pip.PRODUCTID,
        pip.PRINPUTPREFERENCE,
        pjs.PJSOBSSTARTTIME,
        pjs.PJSOBSENDTIME,
        pjs.PJSTIMEOUTTIME,
        pis.PRISFILEHANDLE,
        pis.PRISTEST,
        FN_GET_MILLIS(((pjs.PJSOBSENDTIME+pis.PRISRIGHTOFFSETINTERVAL)-fm.FILESTARTTIME)+PRODUCTCOVERAGEGAPINTERVAL_DS) as FILEJOBCOVERAGE
  from
        FILEMETADATA fm, PRODUCTIONRULE pr, PRODUCTIONJOBSPEC pjs,
        PRINPUTSPEC pis, PRINPUTPRODUCT pip, PRODUCTDESCRIPTION pd, GAZETTEER gaz
  where
        pr.PRRULETYPE <> 'GranuleExact' and
                pis.PRID = pr.PRID and pjs.PRID = pis.PRID and
        pip.PRISID = pis.PRISID and
        fm.PRODUCTID = pip.PRODUCTID and
        fm.PRODUCTID = pd.PRODUCTID and
        fm.FILESTARTTIME >= (pjs.PJSOBSSTARTTIME+pis.PRISLEFTOFFSETINTERVAL) and
        fm.FILESTARTTIME < (pjs.PJSOBSENDTIME+pis.PRISRIGHTOFFSETINTERVAL) and
        fm.FILEENDTIME > (pjs.PJSOBSENDTIME+pis.PRISRIGHTOFFSETINTERVAL) and
        fm.FILEDELETEDFLAG='N' and
        pr.GZID is not null and
        pr.GZID = gaz.GZID and
        ST_Intersects(gaz.GZLOCATIONSPATIAL,fm.FILESPATIALAREA) and
        pis.PRISNEED in ('REQUIRED','TRIGGER','OPTIONAL') and
        pjs.PRODPARTIALJOBID = p_ppjid
UNION
  select
        fm.FILEID,
        pip.PRISID,
        pip.PRIPID,
        pip.PRODUCTID,
        pip.PRINPUTPREFERENCE,
        pjs.PJSOBSSTARTTIME,
        pjs.PJSOBSENDTIME,
        pjs.PJSTIMEOUTTIME,
        pis.PRISFILEHANDLE,
        pis.PRISTEST,
        FN_GET_MILLIS(((pjs.PJSOBSENDTIME+pis.PRISRIGHTOFFSETINTERVAL)-fm.FILESTARTTIME)+PRODUCTCOVERAGEGAPINTERVAL_DS) as FILEJOBCOVERAGE
  from
        FILEMETADATA fm, PRODUCTIONRULE pr, PRODUCTIONJOBSPEC pjs,
        PRINPUTSPEC pis, PRINPUTPRODUCT pip, PRODUCTDESCRIPTION pd
  where
        pr.PRRULETYPE <> 'GranuleExact' and
                pis.PRID = pr.PRID and pjs.PRID = pis.PRID and
        pip.PRISID = pis.PRISID and
        fm.PRODUCTID = pip.PRODUCTID and
        fm.PRODUCTID = pd.PRODUCTID and
        fm.FILESTARTTIME >= (pjs.PJSOBSSTARTTIME+pis.PRISLEFTOFFSETINTERVAL) and
        fm.FILESTARTTIME < (pjs.PJSOBSENDTIME+pis.PRISRIGHTOFFSETINTERVAL) and
        fm.FILEENDTIME > (pjs.PJSOBSENDTIME+pis.PRISRIGHTOFFSETINTERVAL) and
        fm.FILEDELETEDFLAG='N' and
        pr.GZID is null and
        pis.PRISNEED in ('REQUIRED','TRIGGER','OPTIONAL') and
        pjs.PRODPARTIALJOBID = p_ppjid
UNION


/* Before-After */
  select
        fm.FILEID,
        pis.PRISID,
        pip.PRIPID,
        pip.PRODUCTID,
        pip.PRINPUTPREFERENCE,
        pjs.PJSOBSSTARTTIME,
        pjs.PJSOBSENDTIME,
        pjs.PJSTIMEOUTTIME,
        pis.PRISFILEHANDLE,
        pis.PRISTEST,
        FN_GET_MILLIS(((pjs.PJSOBSENDTIME+pis.PRISRIGHTOFFSETINTERVAL)-(pjs.PJSOBSSTARTTIME+pis.PRISLEFTOFFSETINTERVAL))+PRODUCTCOVERAGEGAPINTERVAL_DS) as FILEJOBCOVERAGE
  from
        FILEMETADATA fm, PRODUCTIONRULE pr, PRODUCTIONJOBSPEC pjs,
        PRINPUTSPEC pis, PRINPUTPRODUCT pip, PRODUCTDESCRIPTION pd, GAZETTEER gaz
  where
        pr.PRRULETYPE <> 'GranuleExact' and
                pis.PRID = pr.PRID and pjs.PRID = pis.PRID and
        pip.PRISID = pis.PRISID and
        fm.PRODUCTID = pip.PRODUCTID and
        fm.PRODUCTID = pd.PRODUCTID and
        fm.FILESTARTTIME < (pjs.PJSOBSSTARTTIME+pis.PRISLEFTOFFSETINTERVAL) and
        fm.FILEENDTIME > (pjs.PJSOBSENDTIME+pis.PRISRIGHTOFFSETINTERVAL) and
        fm.FILEDELETEDFLAG='N' and
        pr.GZID is not null and
        pr.GZID = gaz.GZID and
        ST_intersects(gaz.GZLOCATIONSPATIAL,fm.FILESPATIALAREA) and
        pis.PRISNEED in ('REQUIRED','TRIGGER','OPTIONAL') and
        pjs.PRODPARTIALJOBID = p_ppjid
UNION
  select
        fm.FILEID,
        pip.PRISID,
        pip.PRIPID,
        pip.PRODUCTID,
        pip.PRINPUTPREFERENCE,
        pjs.PJSOBSSTARTTIME,
        pjs.PJSOBSENDTIME,
        pjs.PJSTIMEOUTTIME,
        pis.PRISFILEHANDLE,
        pis.PRISTEST,
        FN_GET_MILLIS(((pjs.PJSOBSENDTIME+pis.PRISRIGHTOFFSETINTERVAL)-(pjs.PJSOBSSTARTTIME+pis.PRISLEFTOFFSETINTERVAL))+PRODUCTCOVERAGEGAPINTERVAL_DS) as FILEJOBCOVERAGE
  from
        FILEMETADATA fm, PRODUCTIONRULE pr, PRODUCTIONJOBSPEC pjs,
        PRINPUTSPEC pis, PRINPUTPRODUCT pip, PRODUCTDESCRIPTION pd
  where
        pr.PRRULETYPE <> 'GranuleExact' and
                pis.PRID = pr.PRID and pjs.PRID = pis.PRID and
        pip.PRISID = pis.PRISID and
        fm.PRODUCTID = pip.PRODUCTID and
        fm.PRODUCTID = pd.PRODUCTID and
        fm.FILESTARTTIME < (pjs.PJSOBSSTARTTIME+pis.PRISLEFTOFFSETINTERVAL) and
        fm.FILEENDTIME > (pjs.PJSOBSENDTIME+pis.PRISRIGHTOFFSETINTERVAL) and
        fm.FILEDELETEDFLAG='N' and
        pr.GZID is null and
        pis.PRISNEED in ('REQUIRED','TRIGGER','OPTIONAL') and
        pjs.PRODPARTIALJOBID = p_ppjid
UNION

/* Granule Exact Rule Type */
  select
        fm.FILEID,
        pis.PRISID,
        pip.PRIPID,
        pip.PRODUCTID,
        pip.PRINPUTPREFERENCE,
        pjs.PJSOBSSTARTTIME,
        pjs.PJSOBSENDTIME,
        pjs.PJSTIMEOUTTIME,
        pis.PRISFILEHANDLE,
        pis.PRISTEST,
        FN_GET_MILLIS((fm.FILEENDTIME-fm.FILESTARTTIME)+PRODUCTCOVERAGEGAPINTERVAL_DS) as FILEJOBCOVERAGE
  from
        FILEMETADATA fm, PRODUCTIONRULE pr, PRODUCTIONJOBSPEC pjs,
        PRINPUTSPEC pis, PRINPUTPRODUCT pip, PRODUCTDESCRIPTION pd, GAZETTEER gaz
  where
        pr.PRRULETYPE = 'GranuleExact' and
        pis.PRID = pr.PRID and pjs.PRID = pis.PRID and
        pip.PRISID = pis.PRISID and
        fm.PRODUCTID = pip.PRODUCTID and
        fm.PRODUCTID = pd.PRODUCTID and
        fm.FILESTARTTIME = pjs.PJSOBSSTARTTIME and
        fm.FILEENDTIME = pjs.PJSOBSENDTIME and
        fm.FILEDELETEDFLAG='N' and
        pr.GZID is not null and
        pr.GZID = gaz.GZID and
        ST_Intersects(gaz.GZLOCATIONSPATIAL,fm.FILESPATIALAREA) and
        pis.PRISNEED in ('REQUIRED','TRIGGER','OPTIONAL') and
        pjs.PRODPARTIALJOBID = p_ppjid
UNION
  select
        fm.FILEID,
        pip.PRISID,
        pip.PRIPID,
        pip.PRODUCTID,
        pip.PRINPUTPREFERENCE,
        pjs.PJSOBSSTARTTIME,
        pjs.PJSOBSENDTIME,
        pjs.PJSTIMEOUTTIME,
        pis.PRISFILEHANDLE,
        pis.PRISTEST,
        FN_GET_MILLIS((fm.FILEENDTIME-fm.FILESTARTTIME)+PRODUCTCOVERAGEGAPINTERVAL_DS) as FILEJOBCOVERAGE
  from
        FILEMETADATA fm, PRODUCTIONRULE pr, PRODUCTIONJOBSPEC pjs,
        PRINPUTSPEC pis, PRINPUTPRODUCT pip, PRODUCTDESCRIPTION pd
  where
        pr.PRRULETYPE = 'GranuleExact' and
        pis.PRID = pr.PRID and pjs.PRID = pis.PRID and
        pip.PRISID = pis.PRISID and
        fm.PRODUCTID = pip.PRODUCTID and
        fm.PRODUCTID = pd.PRODUCTID and
        fm.FILESTARTTIME = pjs.PJSOBSSTARTTIME and
        fm.FILEENDTIME = pjs.PJSOBSENDTIME and
        fm.FILEDELETEDFLAG='N' and
        pr.GZID is null and
        pis.PRISNEED in ('REQUIRED','TRIGGER','OPTIONAL') and
        pjs.PRODPARTIALJOBID = p_ppjid
UNION

/* Required Point */
  select
        fm.FILEID,
        pip.PRISID,
        pip.PRIPID,
        pip.PRODUCTID,
        pip.PRINPUTPREFERENCE,
        pjs.PJSOBSSTARTTIME,
        pjs.PJSOBSENDTIME,
        pjs.PJSTIMEOUTTIME,
        pis.PRISFILEHANDLE,
        pis.PRISTEST,
        1 as FILEJOBCOVERAGE
  from
        FILEMETADATA fm, PRODUCTIONRULE pr, PRODUCTIONJOBSPEC pjs,
        PRINPUTSPEC pis, PRINPUTPRODUCT pip, PRODUCTDESCRIPTION pd
  where
        pis.PRID = pr.PRID and pjs.PRID = pis.PRID and
        pip.PRISID = pis.PRISID and
        fm.PRODUCTID = pip.PRODUCTID and
        fm.PRODUCTID = pd.PRODUCTID and
        fm.FILESTARTTIME > (pjs.PJSOBSSTARTTIME+((pjs.PJSOBSENDTIME-pjs.PJSOBSSTARTTIME)/2)+pis.PRISLEFTOFFSETINTERVAL) and
        fm.FILEENDTIME < (pjs.PJSOBSSTARTTIME+((pjs.PJSOBSENDTIME-pjs.PJSOBSSTARTTIME)/2)+pis.PRISRIGHTOFFSETINTERVAL) and
        fm.FILEDELETEDFLAG='N' and
        pr.GZID is null and
        PRISNEED = 'REQUIREDPOINT' and
        pjs.PRODPARTIALJOBID = p_ppjid
  order by
        PRISID,
        PRINPUTPREFERENCE;

return c_jismo;

END;
$$ LANGUAGE plpgsql;
