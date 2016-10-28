--搜索组-用户
SELECT s4,
       s6,
       s1,
       a_s2,
       substr(q1,0,50) AS q1,
       v
FROM
    (SELECT substr(rowkey,0,10) AS sid,
            (count(substr(rowkey,0,10))+1)*count(g1)*count(b2a) AS v  --count(substr(rowkey,0,10)sid频数
    FROM credit_test
    WHERE s9>1
    AND fm=1
         ${hivevar:timecondition}  --b2/b3及时间条件(b2/b2,时间等条件个数不确定，从函数参数中拼接该条件)
GROUP BY substr(rowkey,0,10)
SORT BY v DESC
LIMIT ${hivevar:limit}
) credit
JOIN raws
ON raws.rowkey=credit.sid

