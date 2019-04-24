/* ************************************************************ */
/* name: FN_GET_MILLIS                                          */
/* ************************************************************ */

create or replace FUNCTION FN_GET_MILLIS( v_interval in INTERVAL DAY TO SECOND)
RETURNS bigint AS $$
DECLARE
	v_millis bigint;
BEGIN
v_millis := (
             EXTRACT(Day FROM v_interval) * 86400 +
             EXTRACT(Hour FROM v_interval) * 3600 +
             EXTRACT(Minute FROM v_interval) * 60 +
             EXTRACT(Second FROM v_interval)
          )*1000;
return v_millis;
END;
$$ LANGUAGE plpgsql;
