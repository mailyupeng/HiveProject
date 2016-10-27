--声量来源三天线(例：奥迪A3当天口类，论坛类，文章类/奥迪A3当天的评论，注意在使用窗口函数进行滑动计算时必须根据s3a来分区并按时间降序)
WITH wtmp AS (
        SELECT rowkey,
		           to_date(s6) AS time,
		           cx
		    FROM raws
		    WHERE (s3a=="1" OR s9!="1")
		          AND  (mth>=${hivevar:thismth}
		          AND mth<=${hivevar:lastmth})
		          AND to_date(s6)>=${hivevar:lasttime}
		          AND to_date(s6)<=${hivevar:thistime}),
cx_tmp AS(
    SELECT s3a,
	       mt,
		   sum(c) over(PARTITION BY s3a ORDER BY mt DESC rows between CURRENT row and ${hivevar:num} following) AS s3a_cot
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
				      AND mth>=${hivevar:thismth}
				      AND mth<=${hivevar:lastmth}
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
       (s3a_cot/brdint3_cot) AS per,
      cot AS brd3_cot
FROM
    (SELECT model,
	        time,
			t5.mt,
			cot,
			(sum(cot) over(ORDER BY mt DESC rows between CURRENT row and ${hivevar:num} following)) AS brdint3_cot
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
					 AND (mth>=${hivevar:thismth}
					 AND mth<=${hivevar:lastmth})
					 AND cbrdint3!="") t3
        JOIN wtmp
          ON base64(t3.sid)=base64(substr(wtmp.rowkey,0,16))
         GROUP BY model,time) t5)t5
JOIN cx_tmp ON (cx_tmp.mt=t5.mt)