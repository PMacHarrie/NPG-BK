Added column prestimatedjobduration to productionrule table.

\d productionrule;
                     Table "public.productionrule"
            Column            |            Type             | Modifiers
------------------------------+-----------------------------+-----------
 prid                         | bigint                      | not null
 movcode                      | character varying(8)        |
 wedid                        | integer                     |
 jobpriority                  | integer                     | not null
 algorithmid                  | bigint                      | not null
 gzid                         | bigint                      |
 platformid                   | bigint                      |
 plorbittypeid                | integer                     |
 jobclass                     | integer                     | not null
 prrulename                   | character varying(255)      | not null
 prruletype                   | character varying(22)       | not null
 practiveflag_nsof            | smallint                    |
 practiveflag_cbu             | smallint                    |
 prtemporaryspacemb           | double precision            | not null
 prestimatedram_mb            | double precision            | not null
 prestimatedcpu               | double precision            | not null
 prproductcoverageinterval_ds | interval day to second(5)   |
 prproductcoverageinterval_ym | interval year to month      |
 prstartboundarytime          | timestamp without time zone |
 prruninterval_ds             | interval day to second(5)   |
 prruninterval_ym             | interval year to month      |
 prweathereventdistancekm     | double precision            |
 prorbitstartboundary         | integer                     |
 prproductorbitinterval       | integer                     |
 prwaitforinputinterval_ds    | interval day to second(5)   |
 prdataselectionxml           | xml                         |
 prwaitforinputinterval_ym    | interval day to second(5)   |
 prnotifyopsseconds           | integer                     |
 prestimatedjobduration       | integer                     |


 select
        prId,
        prRuleName,
        round( avg(prJobCPU_util), 4) avg_CPU,
        round( max(prJobMem_UtilMB), 4) max_Mem,
        round( avg(inpVolMB), 4) avg_InVol_MB,
        avg(jobDurationS) avg_jobDuration
into temp1
from
(select
        r.prId,
        r.jobClass,
        prRuleName,
        algorithmName,
        to_char(pjsObsStartTime, 'YYMMDD') obsDate,
        prJobStatus,
        case when prJobCPU_Util  > extract (epoch from prJobCompletionTime - prJobStartTime) then prJobCPU_Util else extract (epoch from prJobCompletionTime - prJobStartTime) end jobCPU_or_Dur,
        prJobCPU_Util,
        extract (epoch from prJobCompletionTime - prJobStartTime) jobDurationS,
        prJobMem_Util / 1024.0 / 1024.0 prJobMem_UtilMB,
        prJobIO_Util / 1024.0 / 1024.0 prJobIO_UtilMB,
        inpVol / 1024.0 / 1024.0 inpVolMB,
        inpCnt,
        prJobCompletionTime,
        prJobStarttime
from
        productionJob j,
        (select prodPartialJobId, sum(fileSize) inpVol, count(distinct i.fileId) inpCnt from jobSpecInput i, fileMetadata f where i.fileId = f.fileId group by prodPartialJobId) t,
        productionJobSpec s,
        productionRule r,
        algorithm a
where
        pjsObsStartTime between '2018-12-22' and '2018-12-29'
        and prJobStatus = 'COMPLETE'
        and j.prodPartialJobId = t.prodPartialJobId
        and s.prodPartialJobId = j.prodPartialJobId
        and r.prId = s.prId
        and r.algorithmId = a.algorithmId
) t
group by
        prId,
        prRuleName
order by
        1, 2


select
        prRuleName, prestimatedcpu, prestimatedjobduration from productionrule
where
        prestimatedcpu is not null
        and prRuleName like '%JPSS%'
order by prId

update productionrule as r
 set
        prtemporaryspacemb = coalesce(t.avg_invol_mb, 0),
     prestimatedram_mb  = coalesce(t.max_mem, 0),
     prestimatedcpu     = coalesce(t.avg_cpu, 0),
     prestimatedjobduration = t.avg_jobduration
from temp1 t
where t.prId = r.prId
;

