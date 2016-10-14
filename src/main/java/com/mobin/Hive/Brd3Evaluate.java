package com.mobin.Hive;

import com.alibaba.fastjson.JSON;
import com.mobin.Hive.Utils.Utils;

import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * Created by MOBIN on 2016/10/8.
 * 品牌总体评价(key=4)
 */
public class Brd3Evaluate {
    /*
        query函数的最后个参数
      * 日线：0
      * 三天线：2
      * 周线：6
      * 月线：1
      * */
    private static HiveDataBaseConnection ds;
    private static Connection con;
    private static PreparedStatement pstm;
    private static String tableName = "bca";


    public static void query(int lastMth,int thisMth,String lastTime,String thisTime, int type,int Num) throws SQLException {
        ds = new HiveDataBaseConnection();
        con = ds.getConnection();
        System.out.println(lastMth + "  " + thisMth);
        StringBuffer sb = new StringBuffer();

        String hql = "";
        if(Num == 0){
            hql = "WITH wtmp AS( "+
                    "SELECT rowkey, "+
                    "to_date(s6) AS time, "+
                    "cx "+
            "FROM raws "+
            "WHERE (s3a==1 "+
                    "OR s9!=1) "+
            "AND (mth>=? "+
                    "AND mth<=?) "+
            "AND to_date(s6)>=? "+
                    "AND to_date(s6)<=?), "+
            "fm_tmp AS(   "+
            "SELECT count(DISTINCT g1) AS g1_cot, "+
            "concat_ws(',',model,time) AS mt, "+
                    "fm, "+
            "count(DISTINCT pid) AS pid_cot "+
            "FROM "+
                    "(SELECT g1, "+
                            "sid, "+
                            "cbrdint3 AS model, "+
                            "fm, "+
                            "base64(substr(rowkey,0,10)) AS pid "+
                            "FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 "+
                            "WHERE (s3a==1 "+
                                    "OR s9!=1) "+
                            "AND cbrdint3!='' "+
                            "AND (mth>=? "+
                                    "AND mth<=?)) t3 "+
            "JOIN wtmp "+
            "ON base64(t3.sid)=base64(substr(wtmp.rowkey,0,16)) "+
            "GROUP BY model, "+
                    "fm, "+
                    "time) "+
            "SELECT SUM(fm_tmp.g1_cot) AS g1_cot, "+
                    "split(fm2_tmp.mt,',')[0] AS model, "+
                    "split(fm2_tmp.mt,',')[1] AS time, "+
                    "((SUM(fm2_tmp.pid_cot)/SUM(fm_tmp.pid_cot))*100) AS per  "+
            "FROM fm_tmp fm2_tmp "+
            "JOIN fm_tmp "+
            "ON(fm_tmp.mt=fm2_tmp.mt) "+
            "WHERE fm2_tmp.fm=2 "+
            "GROUP BY fm2_tmp.mt";

            if(Num == 2 || Num == 6){
               hql = "WITH wtmp AS ( "+
                        "SELECT rowkey, "+
                        "to_date(s6) AS time, "+
                        "cx "+
                "FROM raws "+
                "WHERE (s3a==1 "+
                        "OR s9!=1) "+
                "AND (mth>=? "+
                        "AND mth<=?) "+
                "AND to_date(s6)>=? "+
                        "AND to_date(s6)<=?), "+
                "fm_tmp AS(  "+
                "SELECT count(DISTINCT g1) AS g1_cot, "+
                "count(DISTINCT pid) AS pid, "+
                "concat_ws(',',model,wtmp.time) AS mt, "+
                        "fm "+
                "FROM "+
                        "(SELECT sid, "+
                                "fm, "+
                                "cbrdint3 AS model, "+
                                "base64(substr(rowkey,0,10)) AS pid, "+
                                "g1 "+
                                "FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 "+
                                "WHERE (s3a==1 "+
                                        "OR s9!=1) "+
                                "AND mth>=? AND mth<=?) t1 "+
                "JOIN wtmp "+
                "ON base64(t1.sid)=base64(substr(wtmp.rowkey,0,16)) "+
                "GROUP BY model, "+
                        "wtmp.time, "+
                        "fm), "+
                "all_tmp AS ( "+
                        "SELECT sum(pid) AS pid, "+
                "sum(g1_cot) AS g1_cot, "+
                        "mt "+
                "FROM fm_tmp "+
                "GROUP BY mt)   "+
                "SELECT split(fm2_tmp.mt,',')[0] AS model "+
                       "split(fm2_tmp.mt,',')[1] AS time"+
                        "g1_cot, "+
                        "((fm2_cot/all_cot)*100) AS percents "+
                "FROM "+
                        "(SELECT mt, "+
                                "sum(pid) over(ORDER BY fm_tmp.mt desc rows between CURRENT row and 2 following) AS fm2_cot "+
                                "FROM fm_tmp "+
                                "WHERE fm=2) fm2_tmp   "+
                        "JOIN "+
                "(SELECT mt, "+
                        "sum(pid) over(ORDER BY all_tmp.mt desc rows between CURRENT row and 2 following) AS all_cot,  "+
                "sum(g1_cot) over(ORDER BY all_tmp.mt desc rows between CURRENT row and 2 following) AS g1_cot    "+
                "FROM all_tmp) all_tmp1 "+
                "ON (all_tmp1.mt=fm2_tmp.mt)";
            }else{  //Num == 0

            }

        }
        if(Num == 1){
            hql = "WITH fm_tmp AS (   "+
            "SELECT count(DISTINCT g1) AS g1_cot, "+
            "count(DISTINCT pid) AS pid, "+
            "concat_ws(',',model,mth) AS mt, "+
                    "fm "+
            "FROM "+
                    "(SELECT fm, "+
                            "cbrdint3 AS model, "+
                            "base64(substr(rowkey,0,10)) AS pid, "+
                            "g1, "+
                            "mth "+
                            "FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 "+
                            "WHERE (s3a==1 "+
                                    "OR s9!=1) "+
                            "AND mth>=? "+
                                    "AND mth<=? AND cbrdint3!='') t1 "+
            "GROUP BY model,fm,mth) "+
            "SELECT split(fm2_tmp.mt,',')[0] AS model, "+
                    "split(fm2_tmp.mt,',')[1] AS time"+
                    "g1_cot, "+
                    "((fm2_cot/all_cot)*100) AS per "+
            "FROM "+
                    "(SELECT sum(pid) AS all_cot, "+
                            "sum(g1_cot) AS g1_cot, "+
                            "mt FROM fm_tmp GROUP BY mt) t1 "+
                    "JOIN "+
            "(SELECT sum(pid) AS fm2_cot, "+
                    "mt "+
            "FROM fm_tmp "+
            "WHERE fm=2 "+
            "GROUP BY mt) fm2_tmp "+
            "ON (t1.mt=fm2_tmp.mt)";
        }


        System.out.println(hql);
        pstm = con.prepareStatement(hql);

            pstm.setInt(1,lastMth);
            pstm.setInt(2,thisMth);
            pstm.setString(3,lastTime);
            pstm.setString(4,thisTime);
            pstm.setInt(5,lastMth);
            pstm.setInt(6,lastMth);

        ResultSet rs = pstm.executeQuery();
        List<HiveBean> hiveBean = new ArrayList();
        int i = 1;
        while(rs.next()){
            if(Num == 0 || Num == 1 || Num ==2 || (Num == 6 && (i == 1 || i % 7 == 0))) {
                HiveBean hb = new HiveBean();
                hb.setModel(rs.getString("model"));
                hb.setTime(rs.getString("time"));
                hb.setG1_cot(rs.getInt("g1_cot"));
                hb.setPer(rs.getDouble("per"));
                hb.setType(type);
                hiveBean.add(hb);
            }
            i ++;
        }

        //传输数据
        System.out.println(JSON.toJSONString(hiveBean));
       // Utils.transData(hiveBean);
        pstm.close();
        con.close();
    }

    public static void main(String[] args) throws SQLException, IOException, ParseException {
        Brd3Evaluate brd3Evaluate = new Brd3Evaluate();
        //source.dayLine("162","2016-04-01");
        //brd3Evaluate.threeDayLine("162","2016-04-04");
        //h.weekLine("162","2016-04-04");
        //brd3Evaluate.monthLine("162","2016-04-04",4);

    }
}
