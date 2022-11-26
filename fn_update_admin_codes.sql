CREATE OR REPLACE FUNCTION fn_update_admin_codes(tmp_table_name text)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE r record; tmp_count int; tmp_state_id numeric;tmp_county_id numeric;tmp_zipcode_id numeric;tmp_city_id numeric;
query_string text; _field text;sql_stmt text;
BEGIN
	 raise notice 'Processing : %',tmp_table_name;
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
	 FOR r IN
		EXECUTE 'select * from '|| tmp_table_name ||' where state_id is null;'
	 LOOP
	 	raise notice '%',r.id;
		tmp_state_id=0; tmp_county_id=0; tmp_zipcode_id=0; tmp_city_id=0;
		sql_stmt := 'select tab1.fid from 
		(select * from tigerline_states) as tab1,
		(select * from '||tmp_table_name||' where id='||r.id||') as tab2
		where ST_Intersects(tab2.geom,tab1.geom)';
		execute sql_stmt into tmp_state_id;
-- 		raise notice 'state_id : %',tmp_state_id;
		sql_stmt := 'select tab1.fid from 
		(select * from tigerline_counties) as tab1,
		(select * from '||tmp_table_name||' where id='||r.id||') as tab2
		where ST_Intersects(tab2.geom,tab1.geom)';
		execute sql_stmt into tmp_county_id;
-- 		raise notice 'county_id : %',tmp_county_id;
		sql_stmt := 'select tab1.fid from 
		(select * from tigerline_zipcodes) as tab1,
		(select * from '||tmp_table_name||' where id='||r.id||') as tab2
		where ST_Intersects(tab2.geom,tab1.geom)';
		execute sql_stmt into tmp_zipcode_id;
-- 		raise notice 'zipcode_id : %',tmp_zipcode_id;
		sql_stmt := 'select tab1.fid from 
		(select * from cities_extents) as tab1,
		(select * from '||tmp_table_name||' where id='||r.id||') as tab2
		where ST_Intersects(tab2.geom,tab1.geom)';
		execute sql_stmt into tmp_city_id;
-- 		raise notice 'city_id : %',tmp_city_id;
		if tmp_state_id is null then tmp_state_id=0; end if;
		if tmp_county_id is null then tmp_county_id=0; end if;
		if tmp_zipcode_id is null then tmp_zipcode_id=0; end if;
		if tmp_city_id is null then tmp_city_id=0; end if;
		
		query_string := 'update '||tmp_table_name||' set state_id='||tmp_state_id||', county_id='||tmp_county_id||', zipcode_id='||tmp_zipcode_id||',city_id='||tmp_city_id||' where id='||r.id;
		execute query_string;
-- 		raise notice '%',query_string;
	 END LOOP;
	return 'Script successfully completed for : '||tmp_table_name;
END;
$$


