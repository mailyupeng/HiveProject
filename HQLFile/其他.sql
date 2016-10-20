--一个字段转换成中文
SELECT c.v AS S8a,
       COT
FROM convert c
JOIN
   (SELECT S8a,
           count(1) AS COT
	FROM bca
    WHERE MTH=196
            AND array_contains(BRDINT3,"162")=true
    GROUP BY S8a) bca
  ON (c.k=concat(upper("S8a"),bca.S8a))

------------两个字段转换成中文---------------------
原语句：
SELECT S8a,
       S3a,
	   count(1)
FROM bca
WHERE MTH=196
        AND array_contains(BRDINT3,"162")=true
GROUP BY S8a,
         S3a;

转换后：
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/CONVERT"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT substring(c1.v,0,instr(c1.v,"~")-1) AS s8a,
       c2.v AS s3a,COT
FROM
    (SELECT S8a,
	        S3a,
			count(1) AS COT
	FROM bca
WHERE MTH=196
        AND array_contains(BRDINT3,"162")=true
GROUP BY S8a,
         S3a) bca
JOIN convert c1
  ON (c1.k=concat(upper("s8a"),bca.S8a))
JOIN convert c2
  ON (c2.k=concat(upper("s3a"),bca.S3a))


--不去重后的评论链接(S1只有S表有)
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/16年4月brdint3-c5-s1-1"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT v AS BRDINT3,
       CASE WHEN isnotnull(cc5)=true THEN cc5 END,s1
FROM(
    SELECT s1,
	        cbrdint3,
			cc5
	FROM(
	    SELECT s1,
		        rowkey
		FROM Car_Data_RawS
        WHERE MTH="196") t2
    JOIN(
	    SELECT sid,
	            cbrdint3,
			    cc5
		FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
                 LATERAL VIEW explode(c5) tc5 AS cc5
        WHERE MTH="196" AND (s3a=="1" OR s9!="1")) t1
      ON t1.sid=t2.rowkey) firstjoin
JOIN convert c
  ON (c.k=concat("BRDINT3",cbrdint3))


--brdint3对应的c5的评论数
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/16年4月brdint3-c5-s1-统计"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT v AS BRDINT3,
       s_c5,COUNT(1)
FROM(
    SELECT cbrdint3,
	        s_c5,
			ROW_NUMBER() OVER(PARTITION BY concat(cbrdint3,s_c5)) RN
	FROM(
	    SELECT rowkey
	    FROM Car_Data_RawS
        WHERE MTH="196") t2
    JOIN
       (SELECT sid,
	           cbrdint3,
			   CASE WHEN isnotnull(cc5)=true THEN cc5 END AS s_c5
		FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
                 LATERAL VIEW explode(c5) tc5 AS cc5
        WHERE MTH="196"
		        AND (s3a=="1" OR s9!="1")) t1
      ON t1.sid=t2.rowkey) firstjoin
JOIN convert c
  ON (c.k=concat("BRDINT3",cbrdint3))
WHERE RN=1
GROUP BY v,
         s_c5;



--楼层(q1在S表)
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/16年4月brdint3-c5-q1-4月30号-2"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT DISTINCT concat(t3.v AS BRDINT3,cc5_1,s9,q,s1)
FROM(
    SELECT v,
	       (CASE WHEN isnotnull(cc5)=true THEN cc5 END) AS cc5_1,              --c5:故障
           s9,                                                               --s9：楼层
           (CASE WHEN s9="1" THEN " " ELSE q1 END) AS q,s1
	FROM convert c
	JOIN                                                    --小表放在前面，如果放在后面会发生OOM
        (SELECT cc5,
		        q1,
				s9,
				s1,
				cbrdint3
		FROM
            (SELECT q1,
			        rowkey,
					s1
			FROM Car_Data_RawS
            WHERE to_date(s6)="2016-04-30" AND (s3a=="1" OR s9!="1")) t2
        JOIN
           (SELECT sid,
		           cbrdint3,
				   cc5,
				   s9
			FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
                     LATERAL VIEW explode(c5) tc5 AS cc5
            WHERE mth="196"
			        AND (s3a=="1" OR s9!="1")) t1                                               --mth须与t2中的to_date的月份一致
          ON t1.sid=t2.rowkey) firstjoin
      ON (c.k=concat("BRDINT3",cbrdint3))) t3


--196-162-FM=1的IQS对应的原文(q1)
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/UnEexcel-8-30/196-162-FM=1的IQS对应的原文-3"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT concat_ws("$",c.v,q1)
FROM convert c
JOIN(
    SELECT ciqsint2,
	        q1
	FROM(
	    SELECT rowkey,
		       q1
		FROM car_data_raws
        WHERE mth="196") t1
    JOIN(
	    SELECT sid,
	           ciqsint2
		FROM car_data_bca LATERAL VIEW explode(iqsint2) tiqsint2 AS ciqsint2
        WHERE mth="196"
		         AND fm="1"
				 AND (s3a=="1" OR s9!="1")
				 AND array_contains(brdint2,"162")
				 AND if(ciqsint2=="",false,true)==true) t2
      ON (t1.rowkey=t2.sid)) firstjoin
  ON (c.k=concat(upper("iqsint2"),firstjoin.ciqsint2))




--IQS2原文所对应的各fm的文章数
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/UnExcel-9-18/IQS2原文所对应的各fm的文章数"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT c.v AS iqsint2,
       fm,
	   COT
FROM convert c
JOIN(
    SELECT ciqsint2,
           fm,
		   count(DISTINCT pid) AS COT
	FROM car_data_bca2 LATERAL VIEW explode(iqsint2) tiqsint2 AS ciqsint2
    WHERE mth="196"
	        AND if(ciqsint2=="",false,true)==true
			AND (s3a=="1" OR s9!="1")
    GROUP BY ciqsint2,
	         fm) t
  ON(c.k=concat(upper("iqsint2"),t.ciqsint2));


--车系对应的好评的原文数
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/UnExcel-9-18/车系对应的好评的原文数"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT c.v AS brdint3,
       COT
FROM convert c
JOIN(
    SELECT cbrdint3,
	       count(s9) AS COT
	FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
    WHERE mth="196"
	         AND fm="2"
			 AND (s3a=="1" OR s9!="1")
    GROUP BY cbrdint3) t
  ON (c.k=concat(upper("brdint3"),t.cbrdint3));


--车系对应的好评的人数
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/UnExcel-9-18/车系对应的好评的人数"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT c.v AS brdint3,
       COT
FROM convert c
JOIN(
    SELECT cbrdint3,
	       count(DISTINCT g1) AS COT
	FROM car_data_bca2 LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
    WHERE mth="196"
	        AND fm="2"
			AND (s3a=="1" OR s9!="1")
    GROUP BY cbrdint3) t
  ON (c.k=concat(upper("brdint3"),t.cbrdint3));


--车系对应的文章数
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/UnExcel-9-18/车系对应的文章数"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT c.v AS brdint3,
       COT
FROM convert c
JOIN(
    SELECT cbrdint3,
	        count(pid) AS COT
	FROM car_data_bca2 LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
    WHERE mth="196"
	        AND if(cbrdint3=="",false,true)==true
			AND (s3a=="1" OR s9!="1")
    GROUP BY cbrdint3) t
  ON (c.k=concat(upper("brdint3"),t.cbrdint3));

--201604奥迪A3－ATTIMG2-G1，即统计评价了ATTIMG2各指标的人数
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/201604奥迪A3－ATTIMG2-G1"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT cattimg2,
       count(DISTINCT g1)
FROM bca LATERAL VIEW explode(attimg2) tattimg2 AS cattimg2
WHERE mth="196"
        AND array_contains(brdint3,"162")
GROUP BY cattimg2;



--201604，855，attimg1=“内饰”，g1=“潜龙无限”的原文
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/潜龙无限-1"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY ''
SELECT cattimg1,
       g1,
	   q1,
	   s1
FROM(
    SELECT rowkey,
	       q1,
		   s1
	FROM raws
    WHERE mth="196") t1
JOIN(
    SELECT sid,
	       cattimg1,
		   g1
	FROM bca LATERAL VIEW explode(attimg1) tattimg AS cattimg1
    WHERE mth="196"
	        AND (s3a=="1" OR s9!="1")
			AND array_contains(brdint3,"855")
			AND cattimg1="内饰"
			AND g1="潜龙无限") t2
  ON (substr(t1.rowkey,0,16)=t2.sid)

--每篇文章对应的评论数
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/sid-count-g1"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
SELECT g1,
       c.v AS cx
FROM(
    SELECT bca.g1,
	       ccx
	FROM(
	    SELECT substr(bca.rowkey,0,10),
		       g1
        FROM(
		    SELECT rowkey,
			       sid,
				   s9,
				   g1
			FROM bca
			WHERE mth=196
			        AND array_contains(brdint3,"855")) bca
        JOIN(
		    SELECT substr(rowkey,0,16) AS sid,
			       s1
			FROM raws
			WHERE mth=196) raws
          ON bca.sid=raws.sid) firstjoin
    JOIN(
	    SELECT ccx,
		       g1
		FROM bca LATERAL VIEW explode(cx) tcx AS ccx) bca
      ON bca.g1=firstjoin.g1
    GROUP BY bca.g1,bca.ccx) secondjoin
JOIN convert c
  ON (c.k=concat(upper("cx"),secondjoin.ccx))


