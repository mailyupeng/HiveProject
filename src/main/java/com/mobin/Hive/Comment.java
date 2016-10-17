package com.mobin.Hive;

import com.alibaba.fastjson.JSON;
import com.mobin.Hive.Utils.Utils;


import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

//关键指_评论声量及声量份额（key=1）
public class Comment {
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
            hql ="--日线:评论声量及声量份额\n" +
                    "WITH wtmp AS ( "+
                    "SELECT rowkey, "+
                    "to_date(s6) AS time, "+
                    "cx "+
            "FROM "+subtableName+" "+
            "WHERE (s3a==1 "+
                    "OR s9!=1) "+
            "AND (mth>=? "+
                    "AND mth<=?) "+
            "AND to_date(s6)>=? "+
            "AND to_date(s6)<=?), "+
            "cx_tmp AS  "+
                    "(SELECT concat_ws(',',ccx,time) AS cxt, "+
                            "count(1) AS c, "+
                            "count(DISTINCT g1) AS g1_cot "+
                            "FROM "+
                                    "(SELECT sid, "+
                                            "g1, "+
                                            "ccx "+
                                            "FROM "+tableName+" LATERAL VIEW explode(cx) tcx AS ccx "+
                                            "WHERE (s3a==1 "+
                                                    "OR s9!=1) "+
                                            "AND mth>=? "+
                                            "AND mth<=? "+
                                            "AND ccx!='') t1 "+
                            "JOIN "+
                                    "(SELECT rowkey, "+
                                            "time "+
                                            "FROM wtmp) t4 "+
                            "ON base64(t1.sid)=base64(substr(t4.rowkey,0,16)) "+
                            "GROUP BY ccx, "+
                            "time) "+
            "SELECT model, "+
                    "time, "+
                    "brd3_cot, "+
                    "g1_cot, "+
            "brd3_cot/c AS per "+
            "FROM "+
                    "(SELECT concat_ws(',',v,time) AS cxt, "+
                            "brd3_cot, "+
                           " time, "+
                            "model "+
                            "FROM "+
                                    "(SELECT model, "+
                                            "time, "+
                                            "count(1) AS brd3_cot "+
                                            "FROM "+
                                                    "(SELECT sid, "+
                                                            "cbrdint3 AS model "+
                                                            "FROM "+tableName+" LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 "+
                                                            "WHERE cbrdint3!='' "+
                                                            "AND (mth>=? "+
                                                                   " AND mth<=?)) t3 "+
                                            "JOIN wtmp "+
                                            "ON base64(t3.sid)=base64(substr(wtmp.rowkey,0,16)) "+
                                           " GROUP BY model,time) t5 "+
                            "JOIN cx_brd3 cb "+
                            "ON (cb.k=t5.model)) t6  "+
            "JOIN cx_tmp "+
            "ON(cx_tmp.cxt=t6.cxt)";

        }else if(Num == 2 || Num == 6){
            hql ="--三天线/周线:评论声量及声量份额\n" +
                    "WITH wtmp AS(  "+
                    "SELECT rowkey,  "+
                    "to_date(s6) AS time,  "+
                    "cx  "+
            "FROM "+subtableName+"  "+
            "WHERE  (s3a==1  "+
                    "OR s9!=1)  "+
            "AND (mth>=?  "+
                    "AND mth<=?)  "+
            "AND to_date(s6)>=?  "+
                    "AND to_date(s6)<=?),  "+
            "cx_tmp AS  "+
            "(SELECT concat_ws(',',ccx,time) AS cxt,  "+
            "sum(c) over(PARTITION BY ccx ORDER BY time DESC rows between CURRENT row and "+Num+" following) AS cx_per,  "+
                    "g1_cot  "+
            "FROM "+
                    "(SELECT ccx, "+
                            "time, "+
                            "count(1) AS c, "+
                            "COUNT(DISTINCT g1) AS g1_cot "+
                            "FROM "+
                                    "(SELECT sid, "+
                                            "g1, "+
                                            "ccx "+
                                            "FROM "+tableName+" LATERAL VIEW explode(cx) tcx AS ccx "+
                                            "WHERE (s3a==1 OR s9!=1) AND mth>=? "+
                                                    "AND mth<=? "+
                                                    "AND ccx!='') t1 "+
                            "JOIN (SELECT rowkey, "+
                                    "time "+
                                    "FROM wtmp) t4 "+
                            "ON base64(t1.sid)=base64(substr(t4.rowkey,0,16)) "+
                            "GROUP BY time,ccx) t2) "+
            "SELECT 	split(mt,',')[0] AS model, "+
            "split(mt,',')[1] AS time, "+
            "brdint3_cot AS brd3_cot, "+
                    "g1_cot, "+
                    "brdint3_cot/cx_per AS per "+
            "FROM "+
                    "(SELECT concat_ws(',',v,split(mt,',')[1]) AS cxt,mt, "+
                            "((sum(cot) over(ORDER BY mt desc rows between CURRENT row and "+Num+" following))) AS brdint3_cot "+
            "FROM "+
                    "(SELECT concat_ws(',',model,time) AS mt, "+
                            "count(1) AS cot "+
                            "FROM "+
                                    "(SELECT sid, "+
                                            "cbrdint3 AS model "+
                                            "FROM "+tableName+" LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 "+
                                            "WHERE cbrdint3!='' "+
                                            "AND (s3a==1 "+
                                                    "OR s9!=1) "+
                                            "AND (mth>=? "+
                                                    "AND mth<=?)) t3 "+
                            "JOIN wtmp "+
                            "ON base64(t3.sid)=base64(substr(wtmp.rowkey,0,16)) "+
                            "GROUP BY model, "+
                            "time) t5 "+
            "JOIN cx_brd3 cb "+
            "ON (cb.k=split(mt,',')[0])) t6 "+
            "JOIN cx_tmp "+
            "ON(cx_tmp.cxt=t6.cxt)";
        }else {  //Num == 1
           hql = "--月线:评论声量及声量份额\n" +
                   "WITH cx_tmp AS "+
            "(SELECT concat_ws(',',ccx,mth) AS cxt, "+
            "count(1) AS c "+
            "FROM "+tableName+" LATERAL VIEW explode(cx) tcx AS ccx "+
            "WHERE (s3a==1 OR s9!=1) AND mth>= ? "+
            "AND mth<= ? "+
            "AND ccx!='' "+
            "GROUP BY ccx,mth) "+
            "SELECT model, "+
                    "time, "+
                    "brd3_cot, "+
                    "g1_cot, "+
            "brd3_cot/c AS per "+
            "FROM "+
                    "(SELECT concat_ws(',',v,time) AS cxt, "+
                            "time, "+
                            "model, "+
                            "g1_cot, "+
                            "brd3_cot "+
                            "FROM   "+
                                    "(SELECT model, "+
                                            "time, "+
                                            "count(distinct g1) AS g1_cot, "+
                                            "count(1) AS brd3_cot   "+
                                            "FROM "+
                                                    "(SELECT sid, "+
                                                            "cbrdint3 AS model, "+
                                                            "g1, "+
                                                            "mth AS time "+
                                                            "FROM "+tableName+" LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 "+
                                                            "WHERE (s3a==1 "+
                                                                    "OR s9!=1) "+
                                                            "AND cbrdint3!='' "+
                                                            "AND mth>=? "+
                                                                    "AND mth<=?) t1 "+
                                            "GROUP BY model,time) t5 "+
                            "JOIN cx_brd3 cb "+
                            "ON (cb.k=t5.model)) t6 "+
            "JOIN cx_tmp "+
            "ON(cx_tmp.cxt=t6.cxt)";
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
        }else {  //Num == 1月线
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
                hb.setBrd3_cot(rs.getInt("brd3_cot"));
                hb.setG1_cot(rs.getInt("g1_cot"));
                hb.setPer(rs.getDouble("per"));
                hb.setType(type);
                hiveBean.add(hb);
            }
            i ++;
        }
        //传输数据
       System.out.println(JSON.toJSONString(hiveBean));
        Utils.transData(hiveBean);
        pstm.close();
        ds.close();
    }





    public static void main(String[] args) throws SQLException, IOException, ParseException {
      //  Common common = new Common();
       // h.dayLine("21","162","2016-04-01");  //时间从服务器中获取
//       h.threeDayLine("21","162","2016-04-04");
//       h.weekLine("21","162","2016-04-04");
//        h.monthLine("21","162","2016-04-04");

    }
}
