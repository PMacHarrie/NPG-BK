delete from observatorydataplan_load;
\copy observatorydataplan_load from '/home/ec2-user/orbautoscale/obsdataplan_20190110.txt' CSV DELIMITER ',';
insert into observatorydataplan select platform, site, orbit::int, (aos_date || ' ' || aos_time)::timestamp, (aos_date || ' ' || los_time)::timestamp from observatorydataplan_load ;

