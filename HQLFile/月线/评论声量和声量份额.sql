--评论声量月线
WITH cx_tmp AS
    (SELECT concat_ws(",",ccx,mth) AS cxt,
	        count(1) AS c
    FROM bca LATERAL VIEW explode(cx) tcx AS ccx
	WHERE (s3a==1 OR s9!=1) AND mth>=${hivevar:lastmth}
        	AND mth<=${hivevar:thismth} 
			AND ccx!=""
	GROUP BY ccx,mth)	
SELECT model,
       time,
	   brd3_cot,  
	   g1_cot,
	   brd3_cot/c AS per
FROM
    (SELECT concat_ws(",",v,time) AS cxt,
	          time,
	          model,
			      brd3_cot,
			      g1_cot
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
		             AND mth>=${hivevar:lastmth} 
				     AND mth<=${hivevar:thismth}) t1
        GROUP BY model,time) t5
	JOIN cx_brd3 cb 
	  ON (cb.k=t5.model)) t6
JOIN cx_tmp 
  ON(cx_tmp.cxt=t6.cxt)