--日线
WITH daytmp AS (
    SELECT rowkey,
	       to_date(s6) AS time 
	FROM raws 
	WHERE (s3a=="1" 
	         OR s9!="1") 
			 AND  (mth>=195 
			 AND mth<=196) 
			 AND to_date(s6)>="2016-03-01"
			 AND to_date(s6)<="2016-04-04"),
each_g1 AS    --每天的人数
    (SELECT concat_ws(",",model,daytmp.time) AS mt,
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
				 AND size(attimg2)>=1 
				 AND cbrdint3!="") t1
    JOIN daytmp 
	  ON base64(t1.sid)=base64(substr(daytmp.rowkey,0,16))
    GROUP BY model,
	         daytmp.time),
each_iqs AS       --每天的各个cattimg2对应的数据
    (SELECT cattimg2,
	        concat_ws(",",model,daytmp.time) AS mt,
			count(1) AS attimg2_cot,
			model,
			time 
	FROM
        (SELECT cattimg2,
		        sid,
				cbrdint3 AS model 
		FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
                 LATERAL VIEW explode(attimg2) tattimg2 AS cattimg2 
		WHERE (s3a==1 
		         OR s9!=1) 
				 AND cbrdint3!="" 
				 AND cattimg2!="" 
				 AND mth>=195 
				 AND mth<=196) t1
    JOIN daytmp 
	  ON base64(t1.sid)=base64(substr(daytmp.rowkey,0,20))
    GROUP BY cattimg2,model,daytmp.time)
SELECT model,
       time,
	   cattimg2,
       g1_cot,  --人数
       (attimg2_cot/g1_cot)*100 AS pre 
FROM each_iqs
JOIN each_g1 
  ON(each_g1.mt=each_iqs.mt)






--三天线
WITH daytmp AS (
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
    (SELECT concat_ws(",",model,daytmp.time) AS mt,
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
				 AND size(attimg2)>=1 
				 AND cbrdint3!="") t1
    JOIN daytmp 
	  ON base64(t1.sid)=base64(substr(daytmp.rowkey,0,16))
    GROUP BY model,daytmp.time),
each_iqs AS       --每天的各个cattimg2对应的数据
    (SELECT cattimg2,
	        concat_ws(",",model,daytmp.time) AS mt,
			count(1) AS attimg2_cot 
	FROM
       (SELECT cattimg2,
	           sid,
			   cbrdint3 AS model 
		FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
                 LATERAL VIEW explode(attimg2) tattimg2 AS cattimg2 
		WHERE (s3a==1 
		         OR s9!=1) 
				 AND  cbrdint3!="" 
				 AND cattimg2!="" 
				 AND mth>=195 
				 AND mth<=196) t1
    JOIN daytmp 
	  ON base64(t1.sid)=base64(substr(daytmp.rowkey,0,16))
    GROUP BY cattimg2,
	         model,
			 daytmp.time),
three_each_g1 AS       --每三天的人数
    (SELECT mt,
	        (sum(g1_cot) over(ORDER BY mt DESC rows between CURRENT row and 2 following)) AS three_each_g1_cot 
	FROM each_g1),
three_all_iqs AS   --一天内所有attimg2的总数
    (SELECT mt,
	        sum(attimg2_cot) AS one_day_iqs_cot 
	FROM each_iqs 
	GROUP BY mt)
SELECT split(three_all_iqs.mt,",")[0] AS model, 
       split(three_all_iqs.mt,",")[1] AS time,
	   three_each_g1_cot AS g1_cot,
--每三天内所有attimg2的总数，注意分区
        (sum(one_day_iqs_cot) over(PARTITION BY split(three_all_iqs.mt,",")[0] ORDER BY split(three_all_iqs.mt,",")[1] DESC rows between CURRENT row and 2 following))/three_each_g1_cot*100 AS per 
FROM three_each_g1
JOIN three_all_iqs ON(three_all_iqs.mt=three_each_g1.mt)




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
				 AND (mth BETWEEN 195 AND 196) 
				 AND size(attimg2)>=1 
				 AND cbrdint3!="") t1
    GROUP BY model,time),
each_iqs AS   --某车系每个月的各个attimg2
    (SELECT cattimg2,
	        concat_ws(",",model,time) AS mt,
			count(1) AS attimg2_cot,
			time,
			model 
	FROM 
	    (SELECT cattimg2,
		        sid,
				cbrdint3 AS model,
				mth AS time 
		FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
                 LATERAL VIEW explode(attimg2) tattimg2 AS cattimg2 
		WHERE (s3a==1 
		         OR s9!=1) 
				 AND  cbrdint3!="" 
				 AND cattimg2!="" 
				 AND (mth BETWEEN 195 AND 196)) t1
    GROUP BY cattimg2,
             model,
		     time)
SELECT t5.time,
       t5.model,
	   cattimg2,
	   g1tmp.g1_cot,
       ((attimg2_cot/g1tmp.g1_cot)*100) AS per 
FROM
each_iqs t5
JOIN g1tmp 
  ON(g1tmp.mt=t5.mt) 





