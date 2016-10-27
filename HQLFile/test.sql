--声量来源＝R_S3a/奥迪A3有效评论数*100%
--日线
WITH wtmp AS (SELECT rowkey,
                     to_date(s6) AS time,
					 cx
                FROM raws
                WHERE (s3a=="1"
                      OR s9!="1")
	            	  AND  (mth>=${hivevar:mth1}
	            	  AND mth<=${hivevar:mth2})
	            	  AND to_date(s6)>=${hivevar:day1}
	            	  AND to_date(s6)<=${hivevar:day1}),
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
	   s3a_cot/cot AS per
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
