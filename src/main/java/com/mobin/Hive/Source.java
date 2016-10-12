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
 * Created by MOBIN on 2016/9/28.
 * 关键指_声量来源（key=2）
 */
public class Source {
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
        String hql = "";
        if(Num == 0){   //日线
            hql = "WITH wtmp AS (SELECT rowkey,to_date(s6) AS time,cx FROM raws WHERE (s3a=='1' OR s9!='1') AND  (mth>=? AND mth<=?) AND to_date(s6)>=? AND to_date(s6)<=?), "+
            "cx_tmp AS "+
                    "(SELECT concat_ws(',',model,time) AS mt,s3a,count(1) AS s_cot FROM "+
                                    "(SELECT sid,s3a,cbrdint3 AS model FROM bca "+
                                            "LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 "+
                                            "WHERE (s3a=='1' OR s9!='1') AND mth>=? AND mth<=? AND cbrdint3!='' AND s3a!=4) t1 "+
                            "JOIN wtmp ON base64(t1.sid)=base64(substr(wtmp.rowkey,0,16)) GROUP BY model,time,s3a) "+
            "SELECT model,time,cot AS brd3_cot,s3a,s_cot/cot AS per FROM"+
            "(SELECT concat_ws(',',model,time) AS mt,time,model,count(1) AS cot FROM  "+
                    "(SELECT sid,cbrdint3 AS model FROM bca "+
                            "LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 "+
                            "WHERE (s3a=='1' OR s9!='1') AND (mth>=? AND mth<=?) AND cbrdint3!='') t3 "+
            "JOIN wtmp "+
            "ON base64(t3.sid)=base64(substr(wtmp.rowkey,0,16)) "+
            "GROUP BY model,time) t5 "+
            "JOIN cx_tmp ON(cx_tmp.mt=t5.mt)";

        }else if(Num == 2 || Num == 6){  //三天线
            hql = "WITH wtmp AS (SELECT rowkey,to_date(s6) AS time,cx FROM raws WHERE (s3a=='1' OR s9!='1') AND  (mth>=? AND mth<=?) AND to_date(s6)>=? AND to_date(s6)<=?), "+
            "cx_tmp AS "+
            "(SELECT s3a,mt,sum(c) over(PARTITION BY s3a ORDER BY mt DESC rows between CURRENT row and "+Num+" following) AS cx_per FROM "+
                    "(SELECT count(1) AS c,concat_ws(',',model,time) AS mt,s3a FROM "+
                                    "(SELECT sid,s3a,cbrdint3 AS model FROM bca "+
                                            "LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 "+
                                            "WHERE (s3a=='1' OR s9!='1') AND mth>=? AND mth<=? AND s3a!=4 AND cbrdint3!='') t1 "+
                            "JOIN wtmp ON base64(t1.sid)=base64(substr(wtmp.rowkey,0,16)) GROUP BY model,time,s3a) "+
            "t2) "+
            "SELECT t5.mt,s3a, "+
                    "(cx_per/brd3_cot) AS pre FROM "+
                    "(SELECT t5.mt,cot,(sum(cot) over(ORDER BY mt DESC rows between CURRENT row and "+Num+" following)) AS brd3_cot,model,time  FROM "+
                    "(SELECT concat_ws(',',model,time) AS mt,count(1) AS cot,model,time  FROM "+
                                    "(SELECT sid,cbrdint3 AS model FROM bca "+
                                            "LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 WHERE (s3a=='1' OR s9!='1') AND (mth>=? AND mth<=?) AND cbrdint3!='') t3 "+
                            "JOIN wtmp "+
                            "ON base64(t3.sid)=base64(substr(wtmp.rowkey,0,16)) "+
                            "GROUP BY model,time) t5) t5 "+
            "JOIN cx_tmp ON (cx_tmp.mt=t5.mt) ";
        }else {  //月线
            hql = "WITH cx_tmp AS "+
            "(SELECT s3a,count(1) AS c,concat_ws(',',cbrdint3,mth) AS mt "+
            "FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 "+
            "WHERE (s3a=='1' OR s9!='1') AND  mth>=? AND mth<=? GROUP BY cbrdint3,mth,s3a) "+
            "SELECT model,time,brd3_cot,c/brd3_cot AS per FROM "+
            "(SELECT concat_ws(',',model,time) AS mt,count(1) AS brd3_cot,model,time "+
            "FROM "+
                    "(SELECT sid,cbrdint3 AS model,mth AS time FROM bca "+
                            "LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 WHERE (s3a=='1' OR s9!='1') AND  cbrdint3!='' AND mth>=? AND mth<=?) t1 "+
            "GROUP BY model,time) t5 "+
            "JOIN cx_tmp ON(cx_tmp.mt=t5.mt)";
        }

        System.out.println(hql);
        pstm = con.prepareStatement(hql);
        if(Num == 0 || Num == 2 || Num == 6){
            pstm.setInt(1,lastMth);
            pstm.setInt(2,thisMth);
            pstm.setString(3,lastTime);
            pstm.setString(4,thisTime);
            pstm.setInt(5,lastMth);
            pstm.setInt(6,thisMth);
            pstm.setInt(7,lastMth);
            pstm.setInt(8,thisMth);
        }else if(Num == 1){
            pstm.setInt(1,lastMth);
            pstm.setInt(2,thisMth);
            pstm.setInt(3,lastMth);
            pstm.setInt(4,thisMth);
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
                hb.setPer(rs.getDouble("per"));
                hb.setType(type);
                hiveBean.add(hb);
            }
            i ++;
        }

        //传输数据
        System.out.println(JSON.toJSONString(hiveBean));
      //  Utils.transData(hiveBean);
        pstm.close();
        con.close();
    }

    public static void main(String[] args) throws SQLException, IOException, ParseException {
        Source source = new Source();
       // source.dayLine("162","2016-04-01");
        // h.threeDayLine("162","2016-04-04");
        //h.weekLine("162","2016-04-04");
       // source.monthLine("162","2016-04-04");

    }
}
