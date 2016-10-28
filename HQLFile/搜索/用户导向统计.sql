SELECT cbrdint3,
       COUNT(1) AS cot FROM credit_test
LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3
WHERE fm=1 AND s9>1
AND cbrdint3=${hivevar:condition}
AND ${hivevar:time}