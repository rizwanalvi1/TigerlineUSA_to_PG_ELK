CREATE OR REPLACE FUNCTION fn_update_admin_codes(tmp_table_name text)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE r record; tmp_count int; tmp_state_id numeric;tmp_county_id numeric;tmp_zipcode_id numeric;tmp_city_id numeric;
query_string text; _field text;sql_stmt text; st_datetime time; en_datetime time;
BEGIN
	 st_datetime = now();
	 raise notice 'Start Time : %',st_datetime;
	 raise notice '------------------------------------------------------------------------------';
	 raise notice 'Processing : %',tmp_table_name;
	 raise notice '------------------------------------------------------------------------------';
	 select count(*) into tmp_count from information_schema.columns where table_name=tmp_table_name and column_name='state_id';				
	 if tmp_count > 0 then
			raise notice 'state_id column already exists in the parcel table..';
	  else
			raise notice 'state_id column does not exist..';
			raise notice 'Creating state_id column..';
			execute 'alter table '||tmp_table_name|| ' add state_id numeric';
	 end if;
	 select count(*) into tmp_count from information_schema.columns where table_name=tmp_table_name and column_name='county_id';				
	 if tmp_count > 0 then
			raise notice 'county_id column already exists in the parcel table..';
	  else
			raise notice 'county_id column does not exist..';
			raise notice 'Creating county_id column..';
			execute 'alter table '||tmp_table_name|| ' add county_id numeric';
	 end if;
	 select count(*) into tmp_count from information_schema.columns where table_name=tmp_table_name and column_name='zipcode_id';				
	 if tmp_count > 0 then
			raise notice 'zipcode_id column already exists in the parcel table..';
	  else
			raise notice 'zipcode_id column does not exist..';
			raise notice 'Creating zipcode_id column..';
			execute 'alter table '||tmp_table_name|| ' add zipcode_id numeric';
	 end if;
	 select count(*) into tmp_count from information_schema.columns where table_name=tmp_table_name and column_name='city_id';				
	 if tmp_count > 0 then
			raise notice 'city_id column already exists in the parcel table..';
	  else
			raise notice 'city_id column does not exist..';
			raise notice 'Creating city_id column..';
			execute 'alter table '||tmp_table_name|| ' add city_id numeric';
	 end if;
	 
	 raise notice '------------------------------------------------------------------------------';
	 sql_stmt := 'UPDATE '||tmp_table_name||' as tab1 SET state_id=subquery.fid FROM (select fid,geom from tigerline_states) AS subquery WHERE ST_Intersects(tab1.geom,subquery.geom)';
	 execute sql_stmt;
	 raise notice 'Column state_id has been processed successfully..';
	 
	 sql_stmt := 'UPDATE '||tmp_table_name||' as tab1 SET county_id=subquery.fid FROM (select fid,geom from tigerline_counties) AS subquery WHERE ST_Intersects(tab1.geom,subquery.geom)';
	 execute sql_stmt;
	 raise notice 'Column county_id has been processed successfully..';
	 
	 sql_stmt := 'UPDATE '||tmp_table_name||' as tab1 SET zipcode_id=subquery.fid FROM (select fid,geom from tigerline_zipcodes) AS subquery WHERE ST_Intersects(tab1.geom,subquery.geom)';
	 execute sql_stmt;
	 raise notice 'Column zipcode_id has been processed successfully..';
	 
	 sql_stmt := 'UPDATE '||tmp_table_name||' as tab1 SET city_id=subquery.fid FROM (select fid,geom from cities_extents) AS subquery WHERE ST_Intersects(tab1.geom,subquery.geom)';
	 execute sql_stmt;
	 raise notice 'Column city_id has been processed successfully..';
-- 	 CREATE INDEX texas_idx  ON texas_final  USING GIST (geom);
	 en_datetime = now();
	 raise notice 'End Time : %',en_datetime;
	 return 'Script successfully completed for : '||tmp_table_name||' in duration : '||(en_datetime-st_datetime);
END;
$$


