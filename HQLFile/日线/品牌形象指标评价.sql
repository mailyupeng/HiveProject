--品牌形象指标--日线(credit_test为测试表对应HBase中的car_dataBCATest2)
WITH g1_tmp AS(
      SELECT concat(cbrdint3,s6) AS mt,
             to_date(s6) AS s6,
             cbrdint3,count(DISTINCT g1) AS g1_cot
      FROM credit_test LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
      WHERE size(attimg2)>=1
              AND mth>=${hivevar:lastmth}
			        AND mth<=${hivevar:thismth}
			        AND to_date(s6)>=${hivevar:lasttime}
			        AND to_date(s6)<=${hivevar:thistime}
      GROUP BY s6,cbrdint3
),
att_tmp AS(
      SELECT cbrdint3,
             cattimg2,
             to_date(s6) AS s6,
             concat(cbrdint3,s6) AS mt,
             count(1) AS att_cot --各车系对对应的cattimg2的评论数
      FROM credit_test LATERAL VIEW explode(attimg2) tattimg2 AS cattimg2
                       LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
      WHERE size(attimg2)>=1
              AND cbrdint3 IS NOT NULL
              AND mth>=${hivevar:lastmth}
			        AND mth<=${hivevar:thismth}
			        AND to_date(s6)>=${hivevar:lasttime}
			        AND to_date(s6)<=${hivevar:thistime}
      GROUP BY s6,cbrdint3,cattimg2)
SELECT att_tmp.cbrdint3 AS model,
       att_tmp.s6 AS time,
       g1_cot,
       cattimg2,
       att_cot/g1_cot AS per
FROM g1_tmp
JOIN att_tmp
  ON g1_tmp.mt=att_tmp.mt
