package com.mobin.Hive;

import com.alibaba.fastjson.JSON;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by MOBIN on 2016/10/10.
 */
public class QualityEvalute {
    private static HiveDataBaseConnection ds;
    private static Connection con;
    private static PreparedStatement pstm;
    private static String tableName = "bca";
    private static String subtableName = "raws";



    public static void query(int lastMth,int startMth,String lastTime,String startTime, int type,int Num) throws SQLException {
        ds = new HiveDataBaseConnection();
        con = ds.getConnection();
        System.out.println(lastMth + "  " + startMth);
        StringBuffer sb = new StringBuffer();

        String hql ="";
        if(Num == 0){
            hql =  "--日线:质量评价\n" +
                    "WITH wtmp AS (SELECT rowkey,to_date(s6) AS time,cx FROM raws WHERE (s3a=='1' OR s9!='1') AND  (mth>=? AND mth<=?) AND to_date(s6)>=?AND to_date(s6)<=?), "+
            "each_g1 AS "+
                    "(SELECT concat_ws(',',model,wtmp.time) AS mt,count(DISTINCT g1) AS g1_cot FROM "+
                                    "(SELECT sid,g1,cbrdint3 AS model FROM bca "+
                                            "LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 WHERE (s3a==1 OR s9!=1) AND mth>=? AND mth<=? AND size(iqsint2)>=1 AND cbrdint3!='') t1 "+
                            "JOIN wtmp ON base64(t1.sid)=base64(substr(wtmp.rowkey,0,16)) "+
                            "GROUP BY model,wtmp.time), "+
                    "each_iqs AS "+
                    "(SELECT ciqsint2,concat_ws(',',model,wtmp.time) AS mt,count(1) AS iqsint2_cot,model,time FROM "+
                                    "(SELECT ciqsint2,sid,cbrdint3 AS model FROM bca "+
                                            "LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 "+
                                            "LATERAL VIEW explode(iqsint2) tiqsint2 AS ciqsint2 WHERE (s3a==1 OR s9!=1) AND cbrdint3!='' AND ciqsint2!='' AND mth>=? AND mth<=? AND fm=1) t1 "+
                            "JOIN wtmp ON base64(t1.sid)=base64(substr(wtmp.rowkey,0,16)) "+
                            "GROUP BY ciqsint2,model,wtmp.time), "+
                    "three_all_iqs AS "+
                    "(SELECT mt,sum(iqsint2_cot) AS one_day_iqs_cot  FROM each_iqs GROUP BY mt) "+
            "SELECT model,time,ciqsint2, "+
            "g1_cot, "+
                    "(iqsint2_cot/g1_cot) AS per, "+
                    "(one_day_iqs_cot/g1_cot) AS per1 "+
            "FROM "+
                    "three_all_iqs "+
            "JOIN each_iqs ON(three_all_iqs.mt=each_iqs.mt) "+
            "JOIN each_g1 ON(each_g1.mt=each_iqs.mt) ";

        }else if(Num == 2 || Num == 6){
            hql = "--三天线/日线:质量评价\n" +
                    "WITH wtmp AS (SELECT rowkey,to_date(s6) AS time,cx FROM raws WHERE (s3a=='1' OR s9!='1') AND  (mth>=? AND mth<=?) AND to_date(s6)>=? AND to_date(s6)<=?), "+
            "each_g1 AS "+
                    "(SELECT concat_ws(',',model,wtmp.time) AS mt,count(DISTINCT g1) AS g1_cot FROM "+
                                    "(SELECT sid,g1,cbrdint3 AS model FROM bca "+
                                            "LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 WHERE (s3a==1 OR s9!=1)  AND mth>=? AND mth<=? AND size(iqsint2)>=1 AND cbrdint3!='') t1 "+
                            "JOIN wtmp ON base64(t1.sid)=base64(substr(wtmp.rowkey,0,16)) "+
                            "GROUP BY model,wtmp.time), "+
                    "each_iqs AS "+
                    "(SELECT ciqsint2,concat_ws(',',model,wtmp.time) AS mt,count(1) AS iqsint2_cot FROM "+
                                    "(SELECT ciqsint2,sid,cbrdint3 AS model FROM bca "+
                                            "LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 "+
                                            "LATERAL VIEW explode(iqsint2) tiqsint2 AS ciqsint2 WHERE (s3a==1 OR s9!=1) AND  cbrdint3!='' AND ciqsint2!='' AND mth>=? AND mth<=? AND fm=1) t1 "+
                            "JOIN wtmp ON base64(t1.sid)=base64(substr(wtmp.rowkey,0,16)) "+
                            "GROUP BY ciqsint2,model,wtmp.time), "+
                    "three_each_g1 AS "+
            "(SELECT mt,(sum(g1_cot) over(ORDER BY mt DESC rows between CURRENT row and 2 following)) AS three_each_g1_cot FROM each_g1), "+
            "three_each_iqs AS "+
            "(SELECT ciqsint2,mt,(sum(iqsint2_cot) over(PARTITION BY ciqsint2 ORDER BY mt DESC rows between CURRENT row and 2 following)) AS three_each_iqs_cot  FROM each_iqs), "+
            "three_all_iqs AS "+
                    "(SELECT mt,sum(iqsint2_cot) AS one_day_iqs_cot  FROM each_iqs GROUP BY mt) "+
            "SELECT split(three_all_iqs.mt,',')[0] AS model, split(three_all_iqs.mt,',')[1] AS time,three_each_g1_cot AS g1_cot, "+
                    "three_each_iqs.ciqsint2,"+
            "three_each_iqs_cot/three_each_g1_cot AS per, "+
                    "(sum(one_day_iqs_cot) over(PARTITION BY split(three_all_iqs.mt,',')[0] ORDER BY split(three_all_iqs.mt,',')[1] DESC rows between CURRENT row and 2 following))/three_each_g1_cot AS per1 "+
                    "FROM "+
            "three_each_iqs "+
            "JOIN three_each_g1 ON(three_each_iqs.mt=three_each_g1.mt) "+
            "JOIN three_all_iqs ON(three_all_iqs.mt=three_each_g1.mt) ";
        }else {  //Num == 1
            hql = "--月线:质量评价\n" +
                    "WITH g1tmp AS "+
                    "(SELECT concat_ws(',',model,time) AS mt,count(DISTINCT g1) AS g1_cot FROM "+
                                    "(SELECT sid,g1,cbrdint3 AS model,mth AS time FROM bca "+
                                            "LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 WHERE (s3a==1 OR s9!=1) AND mth>=? AND mth<=? AND size(iqsint2)>=1 AND cbrdint3!='') t1 "+
                            "GROUP BY model,time), "+
                    "each_iqs AS  "+
                    "(SELECT ciqsint2,concat_ws(',',model,time) AS mt,count(1) AS iqsint2_cot,time,model FROM "+
                                   " (SELECT ciqsint2,sid,cbrdint3 AS model,mth AS time FROM bca "+
                                            "LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 "+
                                           "LATERAL VIEW explode(iqsint2) tiqsint2 AS ciqsint2 WHERE (s3a==1 OR s9!=1) AND  cbrdint3!='' AND ciqsint2!='' AND mth>=? AND mth<=? AND fm=1) t1 "+
                           " GROUP BY ciqsint2,model,time), "+
                    "all_iqs AS "+
                    "(SELECT mt,sum(iqsint2_cot) AS all_iqsints_cot FROM each_iqs GROUP BY mt) "+
            "SELECT ciqsint2,t5.time,t5.model,g1tmp.g1_cot, "+
                    "((iqsint2_cot/g1tmp.g1_cot)*100) AS per,   "+
                    "((all_iqsints_cot/g1tmp.g1_cot)*100) AS per1  "+
            "FROM "+
            "each_iqs t5 "+
            "JOIN g1tmp ON(g1tmp.mt=t5.mt) "+
            "JOIN all_iqs ON(g1tmp.mt=all_iqs.mt) ";


        }

        System.out.println(hql);
        pstm = con.prepareStatement(hql);

        if(Num == 0 || Num == 2 || Num == 6){
            pstm.setInt(1,lastMth);
            pstm.setInt(2,startMth);
            pstm.setString(3,lastTime);
            pstm.setString(4,startTime);
            pstm.setInt(5,lastMth);
            pstm.setInt(6,startMth);
            pstm.setInt(7,lastMth);
            pstm.setInt(8,startMth);
        }else {  //Num == 1 月线
            pstm.setInt(1,lastMth);
            pstm.setInt(2,startMth);
            pstm.setInt(3,lastMth);
            pstm.setInt(4,startMth);
        }

        ResultSet rs = pstm.executeQuery();
        List<HiveBean> hiveBean = new ArrayList();
        int i = 1;
        while(rs.next()){
            if(Num == 0 || Num == 1 || Num ==2 || (Num == 6 && (i == 1 || i % 7 == 0))) {
                HiveBean hb = new HiveBean();
                hb.setModel(rs.getString("model"));
                hb.setTime(rs.getString("time"));
                hb.setBrd3_cot(rs.getInt("ciqsint2"));
                hb.setG1_cot(rs.getInt("g1_cot"));
                hb.setPer(rs.getDouble("per"));
                hb.setPer1(rs.getDouble("per1"));
                hb.setType(type);
                hiveBean.add(hb);
            }
            i ++;
        }
        //传输数据
        System.out.println(JSON.toJSONString(hiveBean));
        //Utils.transData(hiveBean);
        pstm.close();
        ds.close();
    }
}
