--声量来源月线
WITH cx_tmp AS (
       SELECT s3a,
	          count(1) AS s3a_cot,   --评论数
			  concat_ws(",",cbrdint3,mth) AS mt
FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 
WHERE (s3a=="1" 
         OR s9!="1") 
		 AND  mth>=${hivevar:lastmth} 
		 AND mth<=${hivevar:thismth} 
GROUP BY cbrdint3,mth,s3a)
SELECT model,
       time,
	   brd3_cot,
	   s3a_cot/brd3_cot AS per,
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
				  AND mth>=${hivevar:lastmth} 
				  AND mth<=${hivevar:thismth}) t1
    GROUP BY model,time) t5
JOIN cx_tmp 
  ON(cx_tmp.mt=t5.mt)



