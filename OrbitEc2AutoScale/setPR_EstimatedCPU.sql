update productionRule r 
set prestimatedcpu = AvgPlusStdev_CPU_Estimate
from 
(select r.prId,
	prRuleName, 
	min(
	case when extract(epoch from prJobCompletionTime - prJobStartTime) >  prJobCPU_Util  then
		extract(epoch from prJobCompletionTime - prJobStartTime)
	else
		prJobCPU_Util
	end) min_CPU_Estimate,
	avg(
	case when extract(epoch from prJobCompletionTime - prJobStartTime) >  prJobCPU_Util  then
		extract(epoch from prJobCompletionTime - prJobStartTime)
	else
		prJobCPU_Util
	end) avg_CPU_Estimate,
	avg( case when extract(epoch from prJobCompletionTime - prJobStartTime) >  prJobCPU_Util  then extract(epoch from prJobCompletionTime - prJobStartTime) else prJobCPU_Util end) + 
	( 1 * stddev( case when extract(epoch from prJobCompletionTime - prJobStartTime) >  prJobCPU_Util  then extract(epoch from prJobCompletionTime - prJobStartTime) else prJobCPU_Util end) )
	AvgPlusStdev_CPU_Estimate,
	max(
	case when extract(epoch from prJobCompletionTime - prJobStartTime) >  prJobCPU_Util  then
		extract(epoch from prJobCompletionTime - prJobStartTime)
	else
		prJobCPU_Util
	end) max_CPU_Estimate,
	stddev(
	case when extract(epoch from prJobCompletionTime - prJobStartTime) >  prJobCPU_Util  then
		extract(epoch from prJobCompletionTime - prJobStartTime)
	else
		prJobCPU_Util
	end) stddev_CPU_Estimate,
	count(*)
from 
	productionRule r, productionJob j, productionJobSpec s
where
 r.prId = s.prId and s.prodPartialJobId = j.prodPartialJobId
 and prJobStatus='COMPLETE'
group by
	1,2
order by
	prRulename
) as t 
where r.prId = t.prId

;
