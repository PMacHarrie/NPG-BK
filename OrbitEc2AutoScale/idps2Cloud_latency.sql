select 
	to_char(fileStartTime, 'YYMMDD HH24:MI') obsMin, 
	split_part(fileName, '_', 1) prodMn,
	split_part(fileName, '_', 2) platform,
	substr(split_part(fileName, '_', 6), 2, 9)::int orbN,
	fileStartTime,
	to_timestamp(substr(split_part(fileName, '_', 7),2, 14), 'YYYYMMDDHH24MISS') idpsCrTime,
	fileInsertTime,
	extract(epoch from fileInsertTime - to_timestamp(substr(split_part(fileName, '_', 7),2, 14), 'YYYYMMDDHH24MISS') ) / 60.0
from 
	filemetadata f
where 
	fileInsertTime >= now() - interval '3' hour
	and fileName like '%npp%h5'
order by fileInsertTime
;
