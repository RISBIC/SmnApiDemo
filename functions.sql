CREATE OR REPLACE FUNCTION array_sort (ANYARRAY)
RETURNS ANYARRAY LANGUAGE SQL
AS $$
SELECT ARRAY(
    SELECT $1[s.i] AS "foo"
    FROM
        generate_series(array_lower($1,1), array_upper($1,1)) AS s(i)
    ORDER BY foo
);
$$;

CREATE OR REPLACE FUNCTION percentile_cont(myarray REAL[], percentile REAL)
RETURNS REAL AS
$$
DECLARE
  ary_cnt INTEGER;
  row_num REAL;
  crn REAL;
  frn REAL;
  calc_result REAL;
  new_array REAL[];
BEGIN
  ary_cnt = array_length(myarray,1);
  row_num = 1 + ( percentile * ( ary_cnt - 1 ));
  new_array = array_sort(myarray);

  crn = ceiling(row_num);
  frn = floor(row_num);

  if crn = frn and frn = row_num then
    calc_result = new_array[row_num];
  else
    calc_result = (crn - row_num) * new_array[frn]
            + (row_num - frn) * new_array[crn];
  end if;

  RETURN calc_result;
END;
$$ LANGUAGE 'plpgsql' IMMUTABLE;

CREATE OR REPLACE FUNCTION speed_summary (period bigint, outstationid integer) RETURNS table(outstationid smallint, period bigint, volume bigint, speed_min smallint, speed_max smallint, speed_avg numeric, speed_stddev numeric, speed_85th_percentile real, _0_to_10 bigint, _11_to_20 bigint, _21_to_30 bigint, _31_to_40 bigint, _41_to_50 bigint, _51_to_60 bigint, _61_to_70 bigint, _70_plus  bigint) AS $$
SELECT
outstationid,
(time - (time % $1)) as period,
count(speed) as volume,
round((min(speed) / 1.6))::smallint as speed_min,
round((max(speed)/1.6))::smallint as speed_max,
round((avg(speed)/1.6)) as speed_avg,
round(stddev((speed/1.6)),2) as speed_stddev,
round((percentile_cont(array_agg(speed), 0.85)/1.6)::numeric)::real as speed_85th_percentile,
count(CASE WHEN (speed/1.6) BETWEEN 0  AND 10 THEN 1 ELSE null END) as _0_to_10,
count(CASE WHEN (speed/1.6) BETWEEN 11 AND 20 THEN 1 ELSE null END) as _11_to_20,
count(CASE WHEN (speed/1.6) BETWEEN 21 AND 30 THEN 1 ELSE null END) as _21_to_30,
count(CASE WHEN (speed/1.6) BETWEEN 31 AND 40 THEN 1 ELSE null END) as _31_to_40,
count(CASE WHEN (speed/1.6) BETWEEN 41 AND 50 THEN 1 ELSE null END) as _41_to_50,
count(CASE WHEN (speed/1.6) BETWEEN 51 AND 60 THEN 1 ELSE null END) as _51_to_60,
count(CASE WHEN (speed/1.6) BETWEEN 61 AND 70 THEN 1 ELSE null END) as _61_to_70,
count(CASE WHEN (speed/1.6) > 70 THEN 1 ELSE null END) as _70_plus
FROM dbdata
WHERE outstationid = $2
GROUP BY outstationid,period;
$$ LANGUAGE 'sql';

CREATE OR REPLACE FUNCTION speed_summary_lane (period bigint, outstationid integer) RETURNS table(outstationid smallint, period bigint, lane smallint, volume bigint, speed_min smallint, speed_max smallint, speed_avg numeric, speed_stddev numeric, speed_85th_percentile real, _0_to_10 bigint, _11_to_20 bigint, _21_to_30 bigint, _31_to_40 bigint, _41_to_50 bigint, _51_to_60 bigint, _61_to_70 bigint, _70_plus  bigint) AS $$
SELECT
outstationid,
(time - (time % $1)) as period,
lane,
count(speed) as volume,
round((min(speed) / 1.6))::smallint as speed_min,
round((max(speed)/1.6))::smallint as speed_max,
round((avg(speed)/1.6)) as speed_avg,
round(stddev((speed/1.6)),2) as speed_stddev,
round((percentile_cont(array_agg(speed), 0.85)/1.6)::numeric)::real as speed_85th_percentile,
count(CASE WHEN (speed/1.6) BETWEEN 0  AND 10 THEN 1 ELSE null END) as _0_to_10,
count(CASE WHEN (speed/1.6) BETWEEN 11 AND 20 THEN 1 ELSE null END) as _11_to_20,
count(CASE WHEN (speed/1.6) BETWEEN 21 AND 30 THEN 1 ELSE null END) as _21_to_30,
count(CASE WHEN (speed/1.6) BETWEEN 31 AND 40 THEN 1 ELSE null END) as _31_to_40,
count(CASE WHEN (speed/1.6) BETWEEN 41 AND 50 THEN 1 ELSE null END) as _41_to_50,
count(CASE WHEN (speed/1.6) BETWEEN 51 AND 60 THEN 1 ELSE null END) as _51_to_60,
count(CASE WHEN (speed/1.6) BETWEEN 61 AND 70 THEN 1 ELSE null END) as _61_to_70,
count(CASE WHEN (speed/1.6) > 70 THEN 1 ELSE null END) as _70_plus
FROM dbdata
WHERE outstationid = $2
GROUP BY outstationid,period,lane;
$$ LANGUAGE 'sql';

CREATE OR REPLACE FUNCTION volume_summary (period bigint, outstationid integer) RETURNS table(outstationid smallint, period bigint, volume bigint, _unclassified bigint, _0_to_52 bigint, _52_to_70 bigint, _70_to_110 bigint, _110_plus bigint) AS $$
SELECT
outstationid,
(time - (time % $1)) as period,
count(classcode) as volume,
count(CASE WHEN classcode = 0 THEN 1 ELSE null END) as _unclassified,
count(CASE WHEN classcode = 1 THEN 1 ELSE null END) as _0_to_52,
count(CASE WHEN classcode = 2 THEN 1 ELSE null END) as _50_to_70,
count(CASE WHEN classcode = 3 THEN 1 ELSE null END) as _70_to_110,
count(CASE WHEN classcode = 4 THEN 1 ELSE null END) as _110_plus
FROM dbdata
WHERE outstationid = $2
GROUP BY outstationid,period;
$$ LANGUAGE 'sql';

CREATE OR REPLACE FUNCTION volume_summary_lane (period bigint, outstationid integer) RETURNS table(outstationid smallint, period bigint, lane smallint, volume bigint, _unclassified bigint, _0_to_52 bigint, _52_to_70 bigint, _70_to_110 bigint, _110_plus bigint) AS $$
SELECT
outstationid,
(time - (time % $1)) as period,
lane,
count(classcode) as volume,
count(CASE WHEN classcode = 0 THEN 1 ELSE null END) as _unclassified,
count(CASE WHEN classcode = 1 THEN 1 ELSE null END) as _0_to_52,
count(CASE WHEN classcode = 2 THEN 1 ELSE null END) as _50_to_70,
count(CASE WHEN classcode = 3 THEN 1 ELSE null END) as _70_to_110,
count(CASE WHEN classcode = 4 THEN 1 ELSE null END) as _110_plus
FROM dbdata
WHERE outstationid = $2
GROUP BY outstationid,period,lane;
$$ LANGUAGE 'sql';