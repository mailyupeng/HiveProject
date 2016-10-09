WITH wtmp AS(
    SELECT rowkey,
		   to_date(s6) AS time,
		cx
    FROM raws 
    WHERE (s3a==1 
	         OR s9!=1) 
			 AND (mth>=195 
			 AND mth<=196) 
			 AND to_date(s6)>="2016-03-01" 
             AND to_date(s6)<="2016-03-02"),
fm_tmp AS(  --每天的人数
    SELECT count(DISTINCT g1) AS g1_cot,
	       model,
		   time,
		   fm,
		   count(DISTINCT pid) AS pid_cot
	FROM
        (SELECT g1,
		        sid,
				cbrdint3 AS model,
				fm,
				base64(substr(rowkey,0,10)) AS pid 
        FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 
        WHERE (s3a==1 
		        OR s9!=1)
				AND cbrdint3=162 
				AND (mth>=195  
				AND mth<=196)) t3
    JOIN wtmp
     ON base64(t3.sid)=base64(substr(wtmp.rowkey,0,16)) 
    GROUP BY model,
	         fm,
			 time)
			 ======
SELECT SUM(fm_tmp.g1_cot) AS g1_cot,
       fm2_tmp.model,
	   fm2_tmp.time,
       ((SUM(fm2_tmp.pid_cot)/SUM(fm_tmp.pid_cot))*100) AS percents --百分比值
FROM fm_tmp fm2_tmp
JOIN fm_tmp 
 ON(fm_tmp.time=fm2_tmp.time) 
WHERE fm2_tmp.fm=2
GROUP BY fm2_tmp.model,
         fm2_tmp.time;


--三天线
WITH wtmp AS (
    SELECT rowkey,
	       to_date(s6) AS time,
		   cx 
    FROM raws 
    WHERE (s3a==1 
             OR s9!=1) 
		     AND (mth>=195 
		     AND mth<=196) 
		     AND to_date(s6)>="2016-03-01" 
		     AND to_date(s6)<="2016-04-04"),
fm_tmp AS(  --每天对应的fm的人数和pid
    SELECT count(DISTINCT g1) AS g1_cot,
	       count(DISTINCT pid) AS pid,
		   wtmp.time AS time,
		   model,
		   fm
    FROM 
        (SELECT sid,
		        fm,
				cbrdint3 AS model,
				base64(substr(rowkey,0,10)) AS pid,
				g1 
		FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
        WHERE (s3a==1 
		        OR s9!=1) 
				AND mth>=195 AND mth<=196 AND cbrdint3=162) t1
    JOIN wtmp 
	 ON base64(t1.sid)=base64(substr(wtmp.rowkey,0,16)) 
	GROUP BY model,
	         fm,
			 wtmp.time),
all_tmp AS (
    SELECT sum(pid) AS pid,
	       sum(g1_cot) AS g1_cot,
		   time,
		   model 
	FROM fm_tmp 
	GROUP BY time,
	         model)  --统计一天内的文章数和人数
SELECT fm2_tmp.model,
       fm2_tmp.time,
	   all_cot,
	   fm2_cot,
	   g1_cot,
       ((fm2_cot/all_cot)*100) AS percents 
FROM
    (SELECT model,
	        time,
			sum(pid) over(ORDER BY fm_tmp.time desc rows between CURRENT row and 2 following) AS fm2_cot 
	FROM fm_tmp 
	WHERE fm=2) fm2_tmp  --三天内fm=2的文章数
JOIN 
    (SELECT time,
           sum(pid) over(ORDER BY all_tmp.time desc rows between CURRENT row and 2 following) AS all_cot, --三天内的总文章数
           sum(g1_cot) over(ORDER BY all_tmp.time desc rows between CURRENT row and 2 following) AS g1_cot   --三天内总人数
    FROM all_tmp) all_tmp1 
ON (all_tmp1.time=fm2_tmp.time)


--月线
WITH fm_tmp AS (   --每天对应的fm的人数和pid
    SELECT count(DISTINCT g1) AS g1_cot,
	       count(DISTINCT pid) AS pid,
		   mth AS time,
		   model,
		   fm 
	FROM 
       (SELECT fm,
	           cbrdint3 AS model,
			   base64(substr(rowkey,0,10)) AS pid,
			   g1,
			   mth 
		FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
        WHERE (s3a==1 
		         OR s9!=1) 
				 AND mth>=195 
				 AND mth<=196 
				 AND cbrdint3=162) t1
        GROUP BY model,fm,mth)
SELECT fm2_tmp.model,
       fm2_tmp.time,
	   all_cot,
	   fm2_cot,
	   g1_cot,
       ((fm2_cot/all_cot)*100) AS percents 
FROM
    (SELECT sum(pid) AS all_cot,
	        sum(g1_cot) AS g1_cot,
			time,model FROM fm_tmp 
	GROUP BY time,
	         model) t1
JOIN 
   (SELECT sum(pid) AS fm2_cot,
           time,
           model 
	FROM fm_tmp 
	WHERE fm=2 
	GROUP BY time,
	         model) fm2_tmp
ON (t1.time=fm2_tmp.time)

