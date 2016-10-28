--品牌形象指标--三/七天(credit_test为测试表对应HBase中的car_dataBCATest2)
WITH g1_tmp AS(
    SELECT mt,
           s6,
           cbrdint3,
           sum(g1_cot) over(ORDER BY t1.mt desc rows between CURRENT row and ${hivevar:num} following) AS g1_cot
    FROM
        (SELECT concat(cbrdint3,s6) AS mt,
                to_date(s6) AS s6,
                cbrdint3,
                count(DISTINCT g1) AS g1_cot
        FROM credit_test LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
        WHERE AND mth>=${hivevar:lastmth}
			        AND mth<=${hivevar:thismth}
			        AND to_date(s6)>=${hivevar:lasttime}
			        AND to_date(s6)<=${hivevar:thistime}
        GROUP BY s6,cbrdint3) t1
),
att_tmp AS(
    SELECT mt,
           s6,
           cbrdint3,
           sum(att_cot) over(ORDER BY t2.mt desc rows between CURRENT row and ${hivevar:num} following) AS att_cot
    FROM
        (SELECT cbrdint3,
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
        GROUP BY s6,cbrdint3,cattimg2) t2
)
SELECT att_tmp.cbrdint3 AS model,
       att_tmp.s6 AS time,
       g1_cot,
       cattimg2,
       att_cot/g1_cot AS per
FROM g1_tmp
JOIN att_tmp
  ON g1_tmp.mt=att_tmp.mt