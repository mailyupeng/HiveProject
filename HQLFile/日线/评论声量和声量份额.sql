--日线-评价声量和声量份额
WITH wtmp AS (
    SELECT rowkey,
	        to_date(s6) AS time,
			cx
	FROM raws
	WHERE (s3a==1
	         OR s9!=1)
			 AND (mth>=${hivevar:lastmth}
			 AND mth<=${hivevar:thismth})
			 AND to_date(s6)>=${hivevar:lasttime}
			 AND to_date(s6)<=${hivevar:thistime}),
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
				 AND mth>=${hivevar:lastmth}
				 AND mth<=${hivevar:thismth}
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
			      AND (mth>=${hivevar:lastmth}
				  AND mth<=${hivevar:thismth} )) t3
        JOIN wtmp
          ON base64(t3.sid)=base64(substr(wtmp.rowkey,0,16))
        GROUP BY model,time) t5
    JOIN cx_brd3 cb           --cx_brd3表中的数据为车型所对应的车系
	  ON (cb.k=t5.model)) t6  --根据车系JOIN得到车型
JOIN cx_tmp
  ON(cx_tmp.cxt=t6.cxt)     --车型+时间作为JOIN条件