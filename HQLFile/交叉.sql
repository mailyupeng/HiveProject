gj,ry,cx,jk,brdint1,brdint2,brdint3两两之间不能交叉
即不能同时存在LATERAL VIEW explode(brdint1) tbrdint1 AS cbrdint1


--1.2015-2016奥迪A3：R_S3a*R_MTH(两字段转成中文)
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/UnEexcel-8-30/2015-2016轿车-紧凑型：R_S3a*R_MTH-S9"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT c1.v,
       c2.v,
	   COT
FROM
    (SELECT s3a,
			mth,
			count(1) AS COT
	FROM bca
    WHERE yr=15
	        OR yr=16
	        AND array_contains(cx,"21")
			AND (s3a=="1" OR s9!="1") --遇到2,3且s9不等于1则继续，
    GROUP BY s3a,
	         mth) bca
JOIN convert c1
  ON (c1.k=concat(upper("S3a"),bca.s3a))   --统一转为大写
JOIN convert c2
  ON (c2.k=concat(upper("mth"),bca.mth))


--2.201603奥迪A3：R_S3a*R_S8a
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/UnEexcel-8-25/201603奥迪A3：R_S3a*R_S8a"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT S3a,
       s,
       COT
FROM
    (SELECT S3a,
	        s,
			COUNT(1) AS COT
	FROM
        (SELECT s3a,
		       (CASE WHEN s3a="1" THEN split(rawg.g5," ")[0] END) s
		FROM bca --g5:帖子发表所属地区(口碑类所对应的地区都在rawg表中的g5字体)
        JOIN car_data_rawg rawg
		  ON(rawg.g1=bca.g1)
        WHERE bca.mth="195"
		         AND array_contains(brdint3,"162")
				 AND (s3a=="1"OR s9!="1")) t1
    JOIN convert c1
	  ON(c1.k=concat(upper("S3a"),s3a))
    GROUP BY S3a,s
UNION ALL
    SELECT c2.v AS S3a,
	       c3.v AS S8a,
		   COT1
	FROM
        (SELECT (CASE WHEN s3a!="1" THEN s3a END) s3a1,
		        s8a,
				count(1) AS COT1
		FROM bca  --s8a：所在省
        WHERE mth="195"
		        AND array_contains(brdint3,"162") AND (s3a=="1" OR s9!="1")
        GROUP BY s3a,
		         s8a) bca1
    JOIN convert c2
	  ON (c2.k=concat(upper("S3a"),bca1.s3a1))
JOIN convert c3
  ON (c3.k=concat(upper("S8a"),bca1.s8a))) u1




--4.201603：R_IQSINT2*R_BRDINT3
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/UnEexcel-8-25/201604：R_IQSINT3*R_BRDINT3"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT c1.v AS iqsint2,
       c2.v AS brdint3,
	   COT
FROM
    (SELECT ciqsint2,
	        cbrdint3,
			count(1) AS COT
	FROM bca LATERAL VIEW explode(iqsint2) tiqsint2 AS ciqsint2
             LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
    WHERE MTH=195
	         AND (s3a=="1" OR s9!="1")
    GROUP BY ciqsint2,
	         cbrdint3) bca
JOIN convert c1
  ON (c1.k=concat(upper("iqsint2"),bca.ciqsint2))
JOIN convert c2
  ON (c2.k=concat(upper("brdint3"),bca.cbrdint3))



--5.2015-2016：R_CX*R_MTH
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/UnEexcel-8-25/2015-2016：R_cx*R_MTH"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT c1.v AS CX,
       c2.v AS mth,
	   COT
FROM
    (SELECT ccx,
	        mth,
			count(1) AS COT
	FROM bca LATERAL VIEW explode(cx) tcx AS ccx
    WHERE (yr=15 OR yr=16)
	             AND (s3a=="1" OR s9!="1")
    GROUP BY ccx,
	         mth) bca
JOIN convert c1
  ON (c1.k=concat(upper("CX"),bca.ccx))
JOIN convert c2
  ON (c2.k=concat(upper("mth"),bca.mth))


--6.201603：R_S3b*R_BRDINT3
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/UnEexcel-8-25/201603：R_S3b*R_BRDINT3"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT s3b,
       c2.v AS brdint3,
	   COT
FROM
    (SELECT s3b,
	        cbrdint3,
			count(1) AS COT
	FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
    WHERE cast(MTH AS INT)=195
	         AND (s3a=="1" OR s9!="1")
    GROUP BY s3b,
	         cbrdint3) bca
JOIN convert c2
  ON (c2.k=concat(upper("brdint3"),bca.cbrdint3))



--8.201603：R_S3a*R_BRDINT3
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/UnEexcel-8-25/201603：R_S3a*R_BRDINT3"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT c1.v AS S3a,
       c2.v AS brdint3,
	   COT
FROM
   (SELECT s3a,
           cbrdint3,
		   count(1) AS COT
	FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
    WHERE mth=195 AND (s3a=="1" OR s9!="1")
    GROUP BY s3a,cbrdint3) bca
JOIN convert c1
  ON (c1.k=concat(upper("S3a"),bca.s3a))
JOIN convert c2
  ON (c2.k=concat(upper("brdint3"),bca.cbrdint3))



--201603：FM*BRDINT3
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/UnEexcel-8-30/201603：FM*R_BRDINT3"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT fm,
       c2.v AS brdint3,
	   COT
FROM
    (SELECT fm,
	        cbrdint3,
			count(1) AS COT
	FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
    WHERE MTH=195
	        AND (s3a=="1" OR s9!="1")
    GROUP BY fm,
	         cbrdint3) bca
JOIN convert c2
  ON (c2.k=concat(upper("brdint3"),bca.cbrdint3))

--201604：FM*BRDINT3
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/UnEexcel-8-30/201604：FM*R_BRDINT3"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT fm,
       c2.v AS brdint3,
	   COT
FROM
    (SELECT fm,
	        cbrdint3,
			count(1) AS COT
	FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
    WHERE MTH=196
	        AND (s3a=="1" OR s9!="1")
    GROUP BY fm,
	         cbrdint3) bca
JOIN convert c2
  ON (c2.k=concat(upper("brdint3"),bca.cbrdint3))


--201603:FM*BRDINT3*IQSINT2
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/UnEexcel-8-30/201603:FM*BRDINT3*IQSINT2"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT s3a,
       fm,
	   c2.v,
	   COT
FROM
    (SELECT s3a,
	        fm,
			cbrdint3,
	        count(1) AS COT
	FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
    WHERE MTH=195
	        AND (s3a=="1" OR s9!="1")
    GROUP BY s3a,
	         fm,
			 cbrdint3) bca
JOIN convert c1
  ON (c1.k=concat(upper("S3a"),bca.s3a))
JOIN convert c2
  ON (c2.k=concat(upper("brdint3"),bca.cbrdint3))




--201603:FM0*BRDINT2*IQSINT2
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/UnEexcel-8-30/201603:FM0*BRDINT2*IQSINT2"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT ciqsint2,
       c2.v,
	   COT
FROM
    (SELECT ciqsint2,
	        cbrdint3,
			count(1) AS COT
	FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
             LATERAL VIEW explode(iqsint2) tiqsint2 AS ciqsint2
    WHERE MTH=195
	         AND (s3a=="1" OR s9!="1")
			 AND fm="0"
    GROUP BY ciqsint2,
	         fm,
			 cbrdint3) bca
JOIN convert c1
  ON (c1.k=concat(upper("iqsint2"),bca.ciqsint2))
JOIN convert c2
  ON (c2.k=concat(upper("brdint3"),bca.cbrdint3))

--201603:FM1*BRDINT2*IQSINT2
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/UnEexcel-8-30/201603:FM1*BRDINT2*IQSINT2"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT ciqsint2,
       c2.v,
	   COT
FROM
   (SELECT ciqsint2,
           cbrdint3,
		   count(1) AS COT
	FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
             LATERAL VIEW explode(iqsint2) tiqsint2 AS ciqsint2
    WHERE mth=195
	         AND (s3a=="1" OR s9!="1")
			 AND fm="1"
    GROUP BY ciqsint2,
	         fm,
			 cbrdint3) bca
JOIN convert c1
  ON (c1.k=concat(upper("iqsint2"),bca.ciqsint2))
JOIN convert c2
  ON (c2.k=concat(upper("brdint3"),bca.cbrdint3))

--201603:FM2*BRDINT2*IQSINT2
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/UnEexcel-8-30/201603:FM2*BRDINT2*IQSINT2"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT ciqsint2,
       c2.v,
	   COT
FROM
   (SELECT ciqsint2,
           cbrdint3,
		   count(1) AS COT
	FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
             LATERAL VIEW explode(iqsint2) tiqsint2 AS ciqsint2
WHERE mth=195
        AND (s3a=="1" OR s9!="1")
		AND fm="2"
GROUP BY ciqsint2,
         fm,
		 cbrdint3) bca
JOIN convert c1
  ON (c1.k=concat(upper("iqsint2"),bca.ciqsint2))
JOIN convert c2
  ON (c2.k=concat(upper("brdint3"),bca.cbrdint3))



--201604：R_IQSINT3*FM
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/UnEexcel-8-30/201604：R_IQSINT3*FM"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT c1.v AS iqsint3,
       fm,
	   COT
FROM
    (SELECT ciqsint3,
	        fm,
			count(1) AS COT
	FROM bca LATERAL VIEW explode(iqsint3) tiqsint3 AS ciqsint3
    WHERE MTH=196
	       AND (s3a=="1" OR s9!="1")
     GROUP BY ciqsint3,fm) bca
JOIN convert c1
  ON (c1.k=concat(upper("iqsint3"),bca.ciqsint3))

--201604关注奥迪A3同时关注某个车系的人数
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/201604关注奥迪A3同时关注某个车系的人数"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT v AS BRDINT3,
       COT
FROM
    convert c
JOIN
    (SELECT tmpbrdint3,
	        count(DISTINCT concat(g1,tmpbrdint3)) AS COT
	FROM   --DISTINCT去重，一个人在多条评论中涉及到某车系但只能算一个
        (SELECT g1,
		        brdint3
		FROM car_bca                                   --g1：网名
        WHERE mth="196"
		        AND array_contains(brdint3,"162")
				AND (s3a=="1" OR s9!="1")) t2  LATERAL VIEW explode(t2.brdint3) tmp AS tmpbrdint3
    GROUP BY tmpbrdint3) t1
  ON(c.k=concat("BRDINT3",t1.tmpbrdint3))

