SELECT rowkey,
       s4,
       s6,
       s1,
       substr(q1,0,50),
       v
FROM
   (SELECT credit.sid,
           (count(1)+1)*count(g1)*count(r_b2a) AS v
   FROM
       (SELECT substr(rowkey,0,10) AS sid,
               count(DISTINCT g1) AS g1_cot
       FROM credit_test
       WHERE ${hivevr:n2_b3_condition}
            ${hivevar:time}
       GROUP BY substr(rowkey,0,10)
       ORDER BY g1_cot DESC) t --按人数进行排序
   JOIN
       (SELECT substr(rowkey,0,10) AS sid,
               r_b2a,
               g1
       FROM credit_test
       WHERE ${hivevr:n2_b3_condition}
            ${hivevar:time}
       ) credit
       GROUP BY bca.sid
       ORDER BY v
       LIMIT ${hivevar:limit}) t2
JOIN raws
  ON raws.rowkey=t2.sid;