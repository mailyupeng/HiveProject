--日线-整车质量评价和产品性能质量评价
WITH wtmp AS (
    SELECT rowkey,
	       to_date(s6) AS time,
		   cx
	FROM raws
	WHERE (s3a=="1"
	         OR s9!="1")
			 AND mth>=${hivevar:lastmth}
			 AND mth<=${hivevar:thismth}
			 AND to_date(s6)>=${hivevar:lasttime}
			 AND to_date(s6)<=${hivevar:thistime}),
each_g1 AS    --每天的人数
    (SELECT concat_ws(",",model,wtmp.time) AS mt,
	        count(DISTINCT g1) AS g1_cot
	FROM
        (SELECT sid,
		        g1,
				cbrdint3 AS model
		FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
		WHERE (s3a==1
		         OR s9!=1)
				 AND mth>=${hivevar:lastmth}
				 AND mth<=${hivevar:thismth}
				 AND size(iqsint2)>=1
				 AND cbrdint3!="") t1
    JOIN wtmp
	  ON base64(t1.sid)=base64(substr(wtmp.rowkey,0,16))
    GROUP BY model,
	         wtmp.time),
each_iqs AS       --每天的各个ciqsint2对应的数据
        (SELECT ciqsint2,
		        concat_ws(",",model,wtmp.time) AS mt,
		        model,
		        time,
				count(1) AS iqsint2_cot
		FROM
            (SELECT ciqsint2,
			        sid,
					cbrdint3 AS model
			FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
                     LATERAL VIEW explode(iqsint2) tiqsint2 AS ciqsint2
			WHERE (s3a==1
			         OR s9!=1)
					 AND cbrdint3!=""
					 AND ciqsint2!=""
					 AND mth>=${hivevar:lastmth}
					 AND mth<=${hivevar:thismth}
					 AND fm=1) t1
        JOIN wtmp
		  ON base64(t1.sid)=base64(substr(wtmp.rowkey,0,16))
         GROUP BY ciqsint2,
		          model,
				  wtmp.time),
three_all_iqs AS   --一天内所有IQS的总数
        (SELECT mt,
		        sum(iqsint2_cot) AS one_day_iqs_cot
		FROM each_iqs
		GROUP BY mt)
SELECT each_iqs.model,
       each_iqs.time,
       ciqsint2,
       g1_cot,  --人数
       (one_day_iqs_cot/g1_cot)*100 AS per,  -- --整车质量评价
       (iqsint2_cot/g1_cot)*100 AS per1 --产品性能评价质量评价
FROM
three_all_iqs
JOIN each_iqs
  ON(three_all_iqs.mt=each_iqs.mt)
JOIN each_g1
  ON(each_g1.mt=each_iqs.mt)
