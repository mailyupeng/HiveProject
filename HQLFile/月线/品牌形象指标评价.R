
t1<-sql(sqlContext,"SELECT sid, g1, cbrdint3 AS model, mth AS time  FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3  WHERE (s3a==1 OR s9!=1) AND (mth BETWEEN 195 AND 196) AND size(attimg2)>=1 AND cbrdint3!=''")
#t1g<-groupBy(t1, t1$model, t1$time)
#g1tmp<-agg(t1g, mt = concat_ws(',',t1$model,t1$time), g1_cot = countDistinct(t1$g1))
# 每个车系(MODEL) 每个月 发表评论的人数
g1tmp<-agg(groupBy(t1, t1$model, t1$time), mt = concat_ws(',',t1$model,t1$time), g1_cot = countDistinct(t1$g1))

t2<-sql(sqlContext,"SELECT cattimg2, sid, cbrdint3 AS model, mth AS time  FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 LATERAL VIEW explode(attimg2) tattimg2 AS cattimg2  WHERE (s3a==1 OR s9!=1) AND (mth BETWEEN 195 AND 196) AND cbrdint3!='' AND cattimg2!='' ")
each_iqs<-agg(groupBy(t2, t2$cattimg2, t2$model, t2$time), mt = concat_ws(',',t2$model,t2$time), attimg2_cot = count(t2$time))

jt<-join(g1tmp, each_iqs2, g1tmp$mt == each_iqs2$mt)
View(limit(jt, 200))



