--品牌总体评价--月线
WITH fm_tmp AS(  --fm评论数
       SELECT concat(cbrdint3,g1) AS cg,
              cbrdint3,
              g1,
              fm,
              mth
       FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
       WHERE mth=${hivevar:lastmth}
             AND mth=${hivevar:thismth}
             AND mth=${hivevar:lasttime}
AND mth=${hivevar:thistime}
 AND size(attimg2)>=1 AND (s3a==1 OR s9!=1) AND  cbrdint3 IS NOT NULL
),
fm1_tmp AS(  --FM1评论数
SELECT mth,concat(cbrdint3,g1) AS cg,cbrdint3,count(1) AS fm FROM
fm_tmp WHERE fm=1
GROUP BY cbrdint3,g1,mth
),
fm2_tmp AS(  ----FM2评论数
SELECT mth,concat(cbrdint3,g1) AS cg,cbrdint3,g1,count(1) AS fm FROM fm_tmp WHERE fm=2
GROUP BY cbrdint3,g1,mth
)
INSERT OVERWRITE LOCAL DIRECTORY "/home/hadoop/result/品牌总体评价-月线"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT t1.mth AS time,t.cbrdint3 AS model,fm2_cot/t1.fm_g1 AS per,t1.fm_g1 AS g1_cot FROM
--好评人数
(SELECT fm2_tmp.cbrdint3,concat(cbrdint3,fm2_tmp.mth) AS mt,
COUNT(fm2_tmp.g1) AS fm2_cot  --fm2人数
FROM fm2_tmp
LEFT JOIN fm1_tmp ON fm1_tmp.cg=fm2_tmp.cg
WHERE fm2_tmp.fm>fm1_tmp.fm OR fm1_tmp.fm IS NULL
GROUP BY fm2_tmp.cbrdint3,fm2_tmp.mth) t
JOIN (SELECT concat(cbrdint3,mth) AS mt,mth,COUNT(DISTINCT fm_tmp.g1) AS fm_g1 --车系对应的人数
FROM fm_tmp GROUP BY cbrdint3,mth) t1 ON t.mt=t1.mt
