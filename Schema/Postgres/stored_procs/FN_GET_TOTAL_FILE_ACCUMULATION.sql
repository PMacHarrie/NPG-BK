/* ********************************************************************** */
/* name: FN_GET_TOTAL_FILE_ACCUMULATION                                   */
/*                                                                        */
/* purpose: Returns the amount of time, in milliseconds, of the input     */
/*          time period that is covered by the files available in the     */
/*          database for a particular product.                            */
/*                                                                        */
/* revised: 20180109, jhansford, creation                                 */
/* ********************************************************************** */

create or replace FUNCTION FN_GET_TOTAL_FILE_ACCUMULATION( v_product_id in bigint, v_input_start_time in timestamp, v_input_end_time in timestamp) 
RETURNS BIGINT
as $$
DECLARE
    v_latest_end_time_seen timestamp;
    v_file_accumulation_millis bigint;
    v_product_coverage_gap interval day to second(5);
	file_times record;
BEGIN    
	/* Time before v_latest_end_time_seen is not added to the running total, * v_file_accumulation_millis.  As each file is looped through in order of * start time, v_latest_end_time_seen is updated, which avoids double * counting the overlap between files.  */
    v_latest_end_time_seen := v_input_start_time;
    v_file_accumulation_millis := 0;
	
    SELECT PRODUCTCOVERAGEGAPINTERVAL_DS INTO v_product_coverage_gap FROM PRODUCTDESCRIPTION WHERE PRODUCTID = v_product_id;

    FOR file_times IN (SELECT FILESTARTTIME, FILEENDTIME FROM FILEMETADATA WHERE PRODUCTID = v_product_id and FILESTARTTIME <= v_input_end_time and FILEENDTIME >= v_input_start_time ORDER BY productId, FILESTARTTIME)
    LOOP
        IF file_times.FILEENDTIME >= v_latest_end_time_seen THEN
            IF file_times.FILESTARTTIME >= v_latest_end_time_seen THEN
                IF file_times.FILEENDTIME <= v_input_end_time THEN
                    /* Inside-Inside */
                    v_file_accumulation_millis := v_file_accumulation_millis + 
			FN_GET_MILLIS(file_times.FILEENDTIME - file_times.FILESTARTTIME + v_product_coverage_gap);
                ELSE
                    /* Inside-After */
                    v_file_accumulation_millis := v_file_accumulation_millis + FN_GET_MILLIS(v_input_end_time - file_times.FILESTARTTIME + v_product_coverage_gap);
                END IF;
            ELSE
                IF file_times.FILEENDTIME <= v_input_end_time THEN
                    /* Before-Inside */
                    v_file_accumulation_millis := v_file_accumulation_millis + FN_GET_MILLIS(file_times.FILEENDTIME - v_latest_end_time_seen + v_product_coverage_gap);
                ELSE
                    /* Before-After */
                    v_file_accumulation_millis := v_file_accumulation_millis + FN_GET_MILLIS(v_input_end_time - v_latest_end_time_seen + v_product_coverage_gap);
                END IF;
            END IF;
            v_latest_end_time_seen := file_times.FILEENDTIME;
        END IF;     
    END LOOP;
    RETURN v_file_accumulation_millis;
END;
$$ LANGUAGE plpgsql;
