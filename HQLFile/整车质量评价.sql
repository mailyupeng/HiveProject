--日线
--奥迪A3”网评IQS =∑ [奥迪A3_IQS二级指标负评数（R_IQSINT2，FM=1）]÷对应车系有效评论受众数)×100
WITH wtmp AS 
    (SELECT rowkey,
	         to_date(s6) AS time,
			 cx 
	FROM raws 
	WHERE (mth>=195 
	         AND mth<=196) 
			 AND to_date(s6)>="2016-03-01" 
			 AND to_date(s6)<="2016-04-01"),
g1tmp AS 
    (SELECT model,
	        time,
			count(DISTINCT g1) AS g1_cot 
	FROM
       (SELECT sid,
	           cbrdint3 AS model,
			   g1 
		FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 
		WHERE cbrdint3=162 
		         AND (mth>=195 
				 AND mth<=196) 
				 AND size(iqsint2)>=1 
				 AND cbrdint3!="") t3
    JOIN wtmp
      ON base64(t3.sid)=base64(substr(wtmp.rowkey,0,16)) 
    GROUP BY model,time)
SELECT t5.model,
       t5.time,
	   iqsint2_cot,
	   g1_cot,
	   ((SUM(iqsint2_cot)/g1_cot)*100) AS percents 
FROM
   (SELECT model,
           time,
		   count(ciqsint2) AS iqsint2_cot FROM
        (SELECT sid,
		        cbrdint3 AS model,
				ciqsint2 
		FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 
                 LATERAL VIEW explode(iqsint2) tiqsint2 AS ciqsint2 
		WHERE cbrdint3=162 
		        AND (mth>=195 
				AND mth<=196) 
				AND fm=1) t3
    JOIN wtmp
      ON base64(t3.sid)=base64(substr(wtmp.rowkey,0,16)) 
    GROUP BY model,time) t5
JOIN g1tmp 
  ON(g1tmp.time=t5.time) 
  GROUP BY t5.model,
	       t5.time,
		   iqsint2_cot,
		   g1_cot;


	  
--三天线
WITH wtmp AS 
    (SELECT rowkey,
	         to_date(s6) AS time,
			 cx 
	FROM raws 
	WHERE (mth>=195 
	         AND mth<=196) 
			 AND to_date(s6)>="2016-03-01" 
			 AND to_date(s6)<="2016-04-04"),
cx_tmp AS 
   (SELECT time,
           sum(c) over(ORDER BY time DESC rows between CURRENT row and 2 following) AS cx_per 
	FROM 
       (SELECT count(1) AS c,
	           time 
		FROM 
            (SELECT sid  
			 FROM bca
             WHERE  mth>=195 
			           AND mth<=196 
					   AND array_contains(cx,"21")) t1
        JOIN (SELECT rowkey,
		             time 
			  FROM wtmp 
			  WHERE array_contains(cx,"21")) t4 
		  ON base64(t1.sid)=base64(substr(t4.rowkey,0,16)) 
		GROUP BY time) t2)
SELECT model,
       t5.time,
	   cx_per,
	   ((sum(cot) over(ORDER BY t5.time desc rows between CURRENT row and 2 following))) AS brdint3_cot,
	   ((sum(cot) over(ORDER BY t5.time desc rows between CURRENT row and 2 following))/cx_per) AS precent 
FROM
    (SELECT model,
            time,
		    count(1) AS cot 
    FROM
        (SELECT sid,
	            cbrdint3 AS model 
	    FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 
	    WHERE cbrdint3=162 
	    AND (mth>=195 
	    AND mth<=196)) t3
    JOIN wtmp
     ON base64(t3.sid)=base64(substr(wtmp.rowkey,0,16)) 
    GROUP BY model,
             time) t5
JOIN cx_tmp 
  ON (cx_tmp.time=t5.time)


--月线
WITH g1tmp AS 
    (SELECT model,
	         time,
			 count(DISTINCT g1) AS g1_cot 
	FROM
       (SELECT sid,
	           g1,
			   cbrdint3 AS model,
			   mth AS time 
		FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 
		WHERE (s3a==1 
		         OR s9!=1) 
				 AND cbrdint3=162 
				 AND mth>=195 
				 AND mth<=196 
				 AND size(iqsint2)>=1 
				 AND cbrdint3!="") t1
    GROUP BY model,time)
SELECT t5.model,
       t5.time,
	   iqsint2_cot,
	   g1_cot,
	   ((sum(iqsint2_cot)/g1_cot)*100) AS percents 
FROM
    (SELECT model,
	        time,count(1) AS iqsint2_cot 
	FROM
        (SELECT sid,
		        cbrdint3 AS model,
				mth AS time 
		FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
                 LATERAL VIEW explode(iqsint2) tiqsint2 AS ciqsint2 
		WHERE (s3a==1 
		        OR s9!=1) 
				AND  cbrdint3=162 
				AND ciqsint2!="" 
				AND mth>=195 
				AND mth<=196 
				AND fm=1) t1
GROUP BY model,
         time) t5
JOIN g1tmp 
  ON(g1tmp.time=t5.time) 
GROUP BY t5.model,t5.time,iqsint2_cot,g1_cot;













