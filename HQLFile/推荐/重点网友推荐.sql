WITH g1_sid_tmp AS(
    SELECT g1,
           COUNT(substr(rowkey,0,10)) AS g1_sid
    FROM credit_test
    WHERE mth BETWEEN ${hivevar:lastMth}
              AND ${hivevar:thisMth}
         AND to_date(s6) BETWEEN ${hivevar:lastTime}
                         AND ${hivevar:thisTime}
    GROUP BY g1
)
SELECT rowkey,
       s2,
       s8a,
       v
       FROM
             (SELECT sid,
                     g1,
                     v+g1_sid AS v
             FROM
                 (SELECT substr(rowkey,0,10) AS sid,
                         g1,
                         count(g1)*count(g1) +count(1)*count(1) AS v
                 FROM credit_test
                 WHERE array_contains(b2juCredit,"61")
                       AND mth BETWEEN ${hivevar:lastMth}
                       AND ${hivevar:thisMth}
                       AND to_date(s6) BETWEEN ${hivevar:lastTime}
                                       AND ${hivevar:thisTime}
                       AND s9>1
                       AND fm=1
                 GROUP BY substr(rowkey,0,10),g1) t1
             JOIN g1_sid_tmp
               ON g1_sid_tmp.g1=t1.g1) credit
       JOIN car_data_rawg rawg
          ON rawg.g1=credit.sid