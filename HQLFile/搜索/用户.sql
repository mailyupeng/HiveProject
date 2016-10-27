--搜索组-用户
SELECT rowkey,
       s4,
       s6,
       s1,
       substr(q1,0,50),
       v
FROM
    (SELECT substr(rowkey,0,10) AS sid,
            (count(substr(rowkey,0,10))+1)*count(g1)*count(b2a) AS v
    FROM credit_test
    WHERE s9>1
        ${hivevar:condition}
    GROUP BY substr(rowkey,0,10)
    SORT BY v DESC
    LIMIT ${hivevar:limit}
) credit
JOIN raws
  ON raws.rowkey=credit.sid;