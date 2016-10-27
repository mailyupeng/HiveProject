--整车质量月线
WITH g1tmp AS (
        SELECT concat_ws(",",model,time) AS mt,
	           count(DISTINCT g1) AS g1_cot 
		FROM
           (SELECT sid,
		           g1,
				   cbrdint3 AS model,
				   mth AS time 
			FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 
			WHERE (s3a==1 
			         OR s9!=1) 
					 AND mth>=${hivevar:lastmth} 
					 AND mth<=${hivevar:thismth} 
					 AND size(iqsint2)>=1 
					 AND cbrdint3!="") t1
        GROUP BY model,
		         time),
each_iqs AS   --某车系每个月的各个iqs_cot
        (SELECT ciqsint2,
		        concat_ws(",",model,time) AS mt,
				count(1) AS iqsint2_cot,
				time,
				model 
		FROM
            (SELECT ciqsint2,
			        sid,
					cbrdint3 AS model,
					mth AS time 
			FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
                     LATERAL VIEW explode(iqsint2) tiqsint2 AS ciqsint2 
			WHERE (s3a==1 
			         OR s9!=1) 
					 AND  cbrdint3!="" 
					 AND ciqsint2!="" 
					 AND mth>=${hivevar:lastmth} 
					 AND mth<=${hivevar:thismth} 
					 AND fm=1) t1
        GROUP BY ciqsint2,
		         model,
				 time),
all_iqs AS   --某车系整个月的iqs_cot
        (SELECT mt,
		        sum(iqsint2_cot) AS all_iqsints_cot 
		FROM each_iqs 
		GROUP BY mt)
--percents：某车系的某月每个IQS2/该车系的人数，percents1：某车系的某月所有IQS2/该车系的人数
SELECT ciqsint2,
       t5.time,
	   t5.model,
	   g1tmp.g1_cot,
       ((iqsint2_cot/g1tmp.g1_cot)*100) AS per,  --产品性能评价质量评价
       ((all_iqsints_cot/g1tmp.g1_cot)*100) AS per1 --整车质量评价
FROM each_iqs t5
JOIN g1tmp 
  ON(g1tmp.mt=t5.mt) 
JOIN all_iqs 
  ON(g1tmp.mt=all_iqs.mt)





