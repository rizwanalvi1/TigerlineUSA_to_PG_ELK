CREATE OR REPLACE FUNCTION public.fn_generate_parcel_partitions() RETURNS text
LANGUAGE 'plpgsql' COST 100 VOLATILE PARALLEL UNSAFE
AS $$
DECLARE r record; query_string text; _field text;
partition_query text; tmp_state text; tmp_county text; tmp_county_raw text;
BEGIN
	 raise notice 'CREATE TABLE parcel_pol(id bigserial , apn text,parcel_id text,address text,area numeric,city_id int,city_name text, county text, state text, zone text, constraint parcel_pol_pkey primary key (id,state,county)) PARTITION BY LIST (state);';
	 raise notice 'SELECT AddGeometryColumn(''public'',''parcel_pol'',''geom'',3857,''MULTIPOLYGON'',2);';
	 
	 FOR r IN
		(select fid as id,name as state_name from tigerline_states /*where name='Alabama'*/ order by name)
	 LOOP
	 	tmp_state := lower(r.state_name);
		tmp_state = replace(tmp_state,' ','_');
		tmp_state = replace(tmp_state,'-','_');
		tmp_state = replace(tmp_state,'commonwealth_of_the_northern_mariana_islands','cw_nmi_');
		
	 	partition_query := 'CREATE TABLE parcel_pol_state_'||tmp_state||' PARTITION OF parcel_pol FOR VALUES IN ('''||r.state_name||''') PARTITION BY LIST(county);';
	 	raise notice '%',partition_query;
		partition_query := 'CREATE TABLE ct_'||tmp_state||'_default PARTITION OF parcel_pol_state_'||tmp_state||' default;';
	 raise notice '%',partition_query;
	 END LOOP;
	 raise notice 'CREATE TABLE parcel_pol_state_default partition of parcel_pol default;';
	 
	 
	 FOR r IN
		(select tab1.*,tab2.name as state_name from
		(select fid,"NAME" as county_name,"STATEFP" from tigerline_counties) as tab1 inner join 
		(select name, statefp from tigerline_states /*where name='Alabama'*/) as tab2 on tab1."STATEFP" = tab2.statefp order by tab1.county_name)
	 LOOP
	 	tmp_county := lower(r.county_name);
		tmp_county = replace(tmp_county,' ','_');
		tmp_county = replace(tmp_county,'-','_');
		tmp_county = replace(tmp_county,'.','');
		tmp_county = REGEXP_REPLACE(tmp_county,'''','_','g');
		
		tmp_county_raw = REGEXP_REPLACE(r.county_name,'''','''''','g');
		
		tmp_state := lower(r.state_name);
		tmp_state = replace(tmp_state,' ','_');
		tmp_state = replace(tmp_state,'-','_');
		tmp_state = replace(tmp_state,'commonwealth_of_the_northern_mariana_islands','cw_nmi_');
		
	 	partition_query := 'CREATE TABLE ct_'||tmp_state||'_'||tmp_county||' PARTITION OF parcel_pol_state_'||tmp_state||' FOR VALUES IN ('''||tmp_county_raw||''');';
	 	raise notice '%',partition_query;		
		
	 END LOOP;
	 
	return 'Script successfully completed..';
END;
$$

select fn_generate_parcel_partitions();
