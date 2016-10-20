--声量来源＝R_S3a/奥迪A3有效评论数*100%
--日线
WITH wtmp AS (SELECT rowkey,
                     to_date(s6) AS time,
					 cx 
FROM raws 
WHERE (s3a=="1" 
          OR s9!="1") 
		  AND  (mth>=195 
		  AND mth<=196) 
		  AND to_date(s6)>="2016-03-01" 
		  AND to_date(s6)<="2016-04-01"),
cx_tmp AS   --brdint3对应的各s3a的总数
        (SELECT concat_ws(",",model,time) AS mt,
		        s3a,count(1) AS s3a_cot 
		FROM 
            (SELECT sid,
			        s3a,
					cbrdint3 AS model 
			FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
            WHERE (s3a=="1" 
			          OR s9!="1") 
					  AND mth>=195 
					  AND mth<=196 
					  AND cbrdint3!="" 
					  AND s3a!=4) t1
        JOIN wtmp 
		  ON base64(t1.sid)=base64(substr(wtmp.rowkey,0,16)) 
		GROUP BY model,
		         time,
				 s3a)
SELECT model,
       time,
	   cot AS brd3_cot,
	   s3a,
	   s3a_cot/cot*100 AS per
FROM
    (SELECT concat_ws(",",model,time) AS mt,
	        time,
			model,
            count(1) AS cot 
	FROM    --cot为各brdint3的总数
        (SELECT sid,
		        cbrdint3 AS model 
		FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 
        WHERE (s3a=="1" 
		         OR s9!="1") 
				 AND (mth>=195 
				 AND mth<=196) 
				 AND cbrdint3!="") t3
    JOIN wtmp
      ON base64(t3.sid)=base64(substr(wtmp.rowkey,0,16)) 
    GROUP BY model,time) t5
JOIN cx_tmp 
  ON(cx_tmp.mt=t5.mt);   --JOIN条件为(model+time)


--三天线(例：奥迪A3当天口类，论坛类，文章类/奥迪A3当天的评论，注意在使用窗口函数进行滑动计算时必须根据s3a来分区并按时间降序)
WITH wtmp AS (
        SELECT rowkey,
		to_date(s6) AS time,cx FROM raws WHERE (s3a=="1" OR s9!="1") AND  (mth>=195 AND mth<=196) AND to_date(s6)>="2016-03-01" AND to_date(s6)<="2016-04-04"),
cx_tmp AS(
    SELECT s3a,
	       mt,
		   sum(c) over(PARTITION BY s3a ORDER BY mt DESC rows between CURRENT row and 2 following) AS s3a_cot 
	FROM 
        (SELECT count(1) AS c,
		        concat_ws(",",model,time) AS mt,
				s3a 
		FROM 
            (SELECT sid,
		            s3a,
				    cbrdint3 AS model 
		    FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 
            WHERE (s3a=="1" 
		              OR s9!="1") 
				      AND mth>=195 
				      AND mth<=196 
				      AND s3a!=4 
				      AND cbrdint3!="") t1
        JOIN wtmp 
		  ON base64(t1.sid)=base64(substr(wtmp.rowkey,0,16)) 
		GROUP BY model,
		         time,
				 s3a)t2)
SELECT model,
       time,
	   s3a,
       (s3a_cot/brdint3_cot)*100 AS pre 
FROM
    (SELECT model,
	        time,
			t5.mt,
			cot,
			(sum(cot) over(ORDER BY mt DESC rows between CURRENT row and 2 following)) AS brdint3_cot 
	FROM
        (SELECT concat_ws(",",model,time) AS mt,
		        count(1) AS cot,
				model,
				time 
		FROM
            (SELECT sid,
			        cbrdint3 AS model 
			FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 
			WHERE (s3a=="1" 
			         OR s9!="1") 
					 AND (mth>=195 
					 AND mth<=196) 
					 AND cbrdint3!="") t3
        JOIN wtmp
          ON base64(t3.sid)=base64(substr(wtmp.rowkey,0,16)) 
         GROUP BY model,time) t5)t5
JOIN cx_tmp ON (cx_tmp.mt=t5.mt)




--月线
WITH cx_tmp AS (
       SELECT s3a,
	          count(1) AS s3a_cot,   --评论数
			  concat_ws(",",cbrdint3,mth) AS mt
FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 
WHERE (s3a=="1" 
         OR s9!="1") 
		 AND  mth>=195 
		 AND mth<=196 
GROUP BY cbrdint3,mth,s3a)
SELECT model,
       time,
	   brd3_cot,
	   s3a_cot/brd3_cot*100 AS per,
	   s3a
FROM
    (SELECT model,
	        time,
			concat_ws(",",model,time) AS mt,
			count(1) AS brd3_cot  --各月的brdint3的总数
    FROM
        (SELECT sid,
		        cbrdint3 AS model,
				mth AS time 
		FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 
		WHERE (s3a=="1" 
		          OR s9!="1") 
				  AND cbrdint3!="" 
				  AND mth>=195 
				  AND mth<=196) t1
    GROUP BY model,time) t5
JOIN cx_tmp 
  ON(cx_tmp.mt=t5.mt);



