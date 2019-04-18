select
        to_char(to_timestamp(substr(split_part(fileName, '_', 7),2, 14), 'YYYYMMDDHH24MISS'), 'HH24:MI') idpsCrTime,
	sum(case when fileName like '%npp%' and substr(productShortName, 1, 1) = 'A'then 1 end) npp_A,
	sum(case when fileName like '%npp%' and substr(productShortName, 1, 1) = 'C'then 1 end) npp_C,
	sum(case when fileName like '%npp%' and substr(productShortName, 1, 1) = 'O'then 1 end) npp_O,
	sum(case when fileName like '%npp%' and substr(productShortName, 1, 1) = 'V'then 1 end) npp_V,
	sum(case when fileName like '%npp%' then 1 end) npp_total,
	sum(case when fileName like '%j01%' and substr(productShortName, 1, 1) = 'A'then 1 end) n20_A,
	sum(case when fileName like '%j01%' and substr(productShortName, 1, 1) = 'C'then 1 end) n20_C,
	sum(case when fileName like '%j01%' and substr(productShortName, 1, 1) = 'O'then 1 end) n20_O,
	sum(case when fileName like '%j01%' and substr(productShortName, 1, 1) = 'V'then 1 end) n20_V,
	sum(case when fileName like '%j01%' then 1 end) n20_total,
	count(*) total
from
        filemetadata f, productDescription p
where
        fileStartTime between '2018-10-19' and '2018-10-20'
        and fileName like '%h5'
	and f.productId = p.productId
group by 
	1
order by 1


;
