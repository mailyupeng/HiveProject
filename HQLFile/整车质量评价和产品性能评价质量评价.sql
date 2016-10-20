--日线
WITH wtmp AS (
    SELECT rowkey,
	       to_date(s6) AS time,
		   cx 
	FROM raws 
	WHERE (s3a=="1" 
	         OR s9!="1") 
			 AND mth>=195 
			 AND mth<=196 
			 AND to_date(s6)>="2016-03-01"
			 AND to_date(s6)<="2016-04-04"),
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
				 AND mth>=195 
				 AND mth<=196 
				 AND size(iqsint2)>=1 
				 AND cbrdint3!="") t1
    JOIN wtmp 
	  ON base64(t1.sid)=base64(substr(wtmp.rowkey,0,16))
    GROUP BY model,
	         wtmp.time),
each_iqs AS       --每天的各个ciqsint2对应的数据
        (SELECT ciqsint2,
		        concat_ws(",",model,wtmp.time) AS mt,
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
					 AND mth>=195 
					 AND mth<=196 
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
SELECT each_iqs.mt,
       ciqsint2,
       g1_cot,  --人数
       (iqsint2_cot/g1_cot)*100 AS precents, --产品性能评价质量评价
       (one_day_iqs_cot/g1_cot)*100 AS precents1  -- --整车质量评价
FROM
three_all_iqs
JOIN each_iqs 
  ON(three_all_iqs.mt=each_iqs.mt)
JOIN each_g1 
  ON(each_g1.mt=each_iqs.mt)





--三天线
WITH wtmp AS (
        SELECT rowkey,
	           to_date(s6) AS time,
			   cx 
		FROM raws 
		WHERE (s3a=="1" 
		         OR s9!="1") 
				 AND  (mth>=195 
				 AND mth<=196) 
				 AND to_date(s6)>="2016-03-01" 
				 AND to_date(s6)<="2016-04-04"),
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
					 AND mth>=195 
					 AND mth<=196 
					 AND size(iqsint2)>=1 
					 AND cbrdint3!="") t1
        JOIN wtmp 
		  ON base64(t1.sid)=base64(substr(wtmp.rowkey,0,16))
        GROUP BY model,wtmp.time),
each_iqs AS       --每天的各个ciqsint2对应的数据
       (SELECT ciqsint2,
	           concat_ws(",",model,wtmp.time) AS mt,
			   count(1) AS iqsint2_cot 
		FROM
            (SELECT ciqsint2,
			        sid,
					cbrdint3 AS model 
			FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
                     LATERAL VIEW explode(iqsint2) tiqsint2 AS ciqsint2 
			WHERE (s3a==1 
			         OR s9!=1) 
					 AND  cbrdint3!="" 
					 AND ciqsint2!="" 
					 AND mth>=195 
					 AND mth<=196 
					 AND fm=1) t1
        JOIN wtmp 
		  ON base64(t1.sid)=base64(substr(wtmp.rowkey,0,16))
        GROUP BY ciqsint2,model,wtmp.time),
three_each_g1 AS       --每三天的人数（整体质量的产品~的值一样）
        (SELECT mt,
		        (sum(g1_cot) over(ORDER BY mt DESC rows between CURRENT row and 2 following)) AS three_each_g1_cot 
		FROM each_g1),
three_each_iqs AS   --每三天对应的iqs2数据（指标11的2016-04-03，2016-04-02，2016-04-01）
        (SELECT ciqsint2,
		        mt,
				(sum(iqsint2_cot) over(PARTITION BY ciqsint2 ORDER BY mt DESC rows between CURRENT row and 2 following)) AS three_each_iqs_cot  
		FROM each_iqs),
three_all_iqs AS   --一天内所有IQS的总数
        (SELECT mt,
		        sum(iqsint2_cot) AS one_day_iqs_cot 
		FROM each_iqs 
		GROUP BY mt)
SELECT split(three_all_iqs.mt,",")[0] AS model,
       split(three_all_iqs.mt,",")[1] AS time,
	   three_each_g1_cot AS g1_cot,
       three_each_iqs.ciqsint2,
       three_each_iqs_cot/three_each_g1_cot,   --整车质量评价
--每三天内所IQS的总数，注意分区
       (sum(one_day_iqs_cot) over(PARTITION BY split(three_all_iqs.mt,",")[0] ORDER BY split(three_all_iqs.mt,",")[1] DESC rows between CURRENT row and 2 following))/three_each_g1_cot*100  --产品性能评价质量评价
FROM three_each_iqs
JOIN three_each_g1 
  ON(three_each_iqs.mt=three_each_g1.mt)
JOIN three_all_iqs 
  ON(three_all_iqs.mt=three_each_g1.mt)




--月线
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
					 AND mth>=195 
					 AND mth<=196 
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
					 AND mth>=195 
					 AND mth<=196 
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





