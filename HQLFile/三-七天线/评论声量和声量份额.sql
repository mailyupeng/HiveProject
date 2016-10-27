--评论声量和声量份额三天线
WITH wtmp AS(
      SELECT rowkey,
	         to_date(s6) AS time,
			 cx
			 FROM raws
			 WHERE  (s3a==1
			          OR s9!=1)
					  AND (mth>=${hivevar:thistime}
			          AND mth<=${hivevar:lastmth})
					  AND to_date(s6)>=${hivevar:lasttime}
					  AND to_date(s6)<=${hivevar:thistime}),
cx_tmp AS
    (SELECT concat_ws(",",ccx,time) AS cxt,
	        sum(c) over(PARTITION BY ccx ORDER BY time DESC rows between CURRENT row and ${hivevar:num} following) AS cx_per,  --三天内ccx对应的总数
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
            WHERE (s3a==1 OR s9!=1) AND mth>=${hivevar:thistime}
			       AND mth<=${hivevar:lastmth}
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
		brdint3_cot/cx_per AS per
FROM
    (SELECT concat_ws(",",v,split(mt,",")[1]) AS cxt,mt,  --v表示车型
	        ((sum(cot) over(ORDER BY mt desc rows between CURRENT row and ${hivevar:num} following))) AS brdint3_cot  --三天的各车系的总数
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
	              AND (mth>=${hivevar:thistime}
		          AND mth<=${hivevar:lastmth})) t3
        JOIN wtmp
          ON base64(t3.sid)=base64(substr(wtmp.rowkey,0,16))
        GROUP BY model,
                 time) t5
    JOIN cx_brd3 cb
	  ON (cb.k=split(mt,",")[0])) t6
JOIN cx_tmp
ON(cx_tmp.cxt=t6.cxt)