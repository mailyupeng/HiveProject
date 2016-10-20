--model:车系
--time：时间
--brd3_cot：车系对应的评论数
--g1_cot：关注某车系的人数
--日线
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
			 AND to_date(s6)<="2016-04-01"),
cx_tmp AS   --cx每天对应的总数
    (SELECT concat_ws(",",ccx,time) AS cxt,
	        count(1) AS c,
			count(DISTINCT g1) AS g1_cot 
	FROM 
        (SELECT sid,
		        g1,
				ccx 
		FROM bca LATERAL VIEW explode(cx) tcx AS ccx
        WHERE (s3a==1 
		         OR s9!=1) 
				 AND mth>=195 
				 AND mth<=196 
				 AND ccx!="") t1
    JOIN 
	    (SELECT rowkey,
		        time 
	    FROM wtmp) t4 
      ON base64(t1.sid)=base64(substr(t4.rowkey,0,16)) 
    GROUP BY ccx,
	         time)
SELECT model,
       time,
	   brd3_cot,
	   g1_cot,
	   brd3_cot/c*100 AS per     --(根据车系JOIN cx_brd3表得到对应的的车型，再用车型+时间去JOIN cx_tmp表)
FROM
    (SELECT concat_ws(",",v,time) AS cxt,
	        brd3_cot,
			time,
			model --v表示车型(cx)
    FROM
        (SELECT model,
	           time,
			   count(1) AS brd3_cot   --某车系每天对应的总数
        FROM
            (SELECT sid,
			        cbrdint3 AS model 
			FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 
			WHERE cbrdint3!="" 
			      AND (mth>=195 
				  AND mth<=196)) t3
        JOIN wtmp
          ON base64(t3.sid)=base64(substr(wtmp.rowkey,0,16)) 
        GROUP BY model,time) t5
    JOIN cx_brd3 cb           --cx_brd3表中的数据为车型所对应的车系
	  ON (cb.k=t5.model)) t6  --根据车系JOIN得到车型
JOIN cx_tmp 
  ON(cx_tmp.cxt=t6.cxt);      --车型+时间作为JOIN条件


	
--三天线
WITH wtmp AS(
      SELECT rowkey,
	         to_date(s6) AS time,
			 cx 
			 FROM raws 
			 WHERE  (s3a==1 
			          OR s9!=1) 
					  AND (mth>=195 
			          AND mth<=196) 
					  AND to_date(s6)>="2016-03-01" 
					  AND to_date(s6)<="2016-04-04"),
cx_tmp AS 
    (SELECT concat_ws(",",ccx,time) AS cxt,
	        sum(c) over(PARTITION BY ccx ORDER BY time DESC rows between CURRENT row and 2 following) AS cx_per,  --三天内ccx对应的总数
	        g1_cot  
	FROM 
       (SELECT ccx,
	           time,
	           count(1) AS c,   --每天ccx对应的总数
			   COUNT(DISTINCT g1) AS g1_cot 
		FROM 
            (SELECT sid,
                   g1,
                   ccx				   
		    FROM bca LATERAL VIEW explode(cx) tcx AS ccx
            WHERE (s3a==1 OR s9!=1) AND mth>=195 
			       AND mth<=196 
				   AND ccx!="") t1
        JOIN (SELECT rowkey,
		             time 
			  FROM wtmp) t4 
		  ON base64(t1.sid)=base64(substr(t4.rowkey,0,16)) 
		GROUP BY time,ccx) t2) 
SELECT 	split(mt,",")[0] AS model,
        split(mt,",")[1] AS time,
		brdint3_cot AS brd3_cot,
		g1_cot,
		brdint3_cot/cx_per*100 AS per 
FROM	
    (SELECT concat_ws(",",v,split(mt,",")[1]) AS cxt,mt,  --v表示车型
	        ((sum(cot) over(ORDER BY mt desc rows between CURRENT row and 2 following))) AS brdint3_cot  --三天的各车系的总数
    FROM
        (SELECT concat_ws(",",model,time) AS mt,
		        count(1) AS cot   --每天的车系的总数
        FROM
            (SELECT sid,
	                cbrdint3 AS model 
	        FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 
	        WHERE cbrdint3!="" 
			      AND (s3a==1 
		          OR s9!=1) 
	              AND (mth>=195 
		          AND mth<=196)) t3
        JOIN wtmp
          ON base64(t3.sid)=base64(substr(wtmp.rowkey,0,16)) 
        GROUP BY model,
                 time) t5
    JOIN cx_brd3 cb 
	  ON (cb.k=split(mt,",")[0])) t6
JOIN cx_tmp 
  ON(cx_tmp.cxt=t6.cxt);      --根据车型+时间来JOIN		 
		 
		
		


--月线
WITH cx_tmp AS 
    (SELECT concat_ws(",",ccx,mth) AS cxt,
	        count(1) AS c
    FROM bca LATERAL VIEW explode(cx) tcx AS ccx
	WHERE (s3a==1 OR s9!=1) AND mth>=195
        	AND mth<=196 
			AND ccx!=""
	GROUP BY ccx,mth)	
SELECT model,
       time,
	   brd3_cot,  
	   g1_cot
	   brd3_cot/c*100 AS per
FROM
    (SELECT concat_ws(",",v,time) AS cxt,
	        time,
	        model,
			brd3_cot 
	FROM  --v表示车型
        (SELECT model,
                time,
				count(distinct g1) AS g1_cot,
		        count(1) AS brd3_cot  --每个月的各每车的数据
	    FROM
            (SELECT sid,
	                cbrdint3 AS model,
					g1,
			        mth AS time
		    FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 
		    WHERE (s3a==1 
			         OR s9!=1) 
					 AND cbrdint3!=""
		             AND mth>=195 
				     AND mth<=196) t1
        GROUP BY model,time) t5
	JOIN cx_brd3 cb 
	  ON (cb.k=t5.model)) t6
JOIN cx_tmp 
  ON(cx_tmp.cxt=t6.cxt); 
	




--测试各天的数
SELECT time,
       count(1),
	   s3a 
FROM 
   (SELECT sid,
           s3a 
	FROM bca
    WHERE mth>=195 
	       AND mth<=196 
		   AND array_contains(brdint3,"162") 
		   AND s3a!=4) t1
JOIN 
   (SELECT rowkey,
           to_date(s6) AS time 
	FROM raws 
	WHERE (mth>=195 
	        AND mth<=196) 
			AND to_date(s6)>="2016-03-01" 
			AND to_date(s6)<="2016-04-04") t2
   ON base64(t1.sid)=base64(substr(t2.rowkey,0,16))
GROUP BY time,
         s3a


		 
		 
--cx的总数		
WITH wtmp AS 
     (SELECT rowkey,
	         to_date(s6) AS time,
			 cx 
			 FROM raws 
			 WHERE (s3a==1 
                      OR s9!=1) 
		              AND(mth>=195 
			          AND mth<=196) 
					  AND to_date(s6)>="2016-03-01" 
					  AND to_date(s6)<="2016-04-04")
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/评论声量-cx_cot-1"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
       SELECT ccx,
	           time,
	           count(1) AS c,   --每天ccx对应的总数
			   COUNT(DISTINCT g1) AS g1_cot 
		FROM 
           (SELECT sid,
                   g1,
                   ccx				   
		    FROM bca LATERAL VIEW explode(cx) tcx AS ccx
            WHERE mth>=195 
			       AND mth<=196 
				   AND ccx!="") t1
        JOIN (SELECT rowkey,
		             time 
			  FROM wtmp) t4 
		  ON base64(t1.sid)=base64(substr(t4.rowkey,0,16)) 
		GROUP BY time,ccx
	
	




