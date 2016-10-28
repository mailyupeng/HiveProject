WITH tmp AS(
      SELECT substr(rowkey,0,10) AS sid,
             cbrdint3 FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
      WHERE mth=${hivevar.time}
      AND s9>1 AND fm=1
),
cot_tmp AS(
      SELECT sid,
             count(DISTINCT cbrdint3) AS cot
      FROM tmp
      GROUP BY sid
),
brd3_cot_tmp AS(
      SELECT sid,
             count(DISTINCT cbrdint3) AS brd3_cot
      FROM tmp
      WHERE cbrdint3=${hivevar:brdint3}
      GROUP BY sid
)
SELECT rowkey,
       s4,
       s6,
       s1,
       substr(q1,0,50),
       v
FROM(
    SELECT t1.sid,
           brd3_cot/cot AS v
    FROM cot_tmp t1
    JOIN brd3_cot_tmp t2
      ON t1.sid=t2.sid
    ORDER BY v DESC) credit
JOIN raws
   ON raws.rowkey=credit.sid;