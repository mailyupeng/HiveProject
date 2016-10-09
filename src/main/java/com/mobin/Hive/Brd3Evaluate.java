package com.mobin.Hive;

import com.mobin.Hive.Utils.Utils;

import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * Created by MOBIN on 2016/10/8.
 * 品牌总体评价
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
    private PreparedStatement pstm;
    private static String tableName = "bca";

    //（日线）每天关注该车系的人数,时间轴为30天
    public  void dayLine(String model,String thisTime) throws SQLException, IOException, ParseException {
        //根据开始时间推出上个月的时间
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(Date.valueOf(thisTime));

        int thisMth = Utils.disMonth(calendar);   //这个月的编号
        int lastMth = thisMth-1;            //上个月的编号

        calendar.roll(Calendar.MONTH,false);   //上滚一个月
        String lastTime = new Date(calendar.getTime().getTime()).toString();//上个月的时间

        query(model,lastMth,thisMth,lastTime,thisTime,1,0);
    }

    //（三天线）时间轴为31天内30个“三天线”数据
    public void threeDayLine(String model,String thisTime) throws ParseException, SQLException {
        //根据开始时间推出三天前的时间
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(Date.valueOf(thisTime));
        int thisMth = Utils.disMonth(calendar);   //今天对应的mth编号

        calendar.add(Calendar.DATE,-32);   //上滚三天
        String lastTime = new Date(calendar.getTime().getTime()).toString();//三天前的时间
        int lastMth = Utils.disMonth(calendar);            //三天前的mth编号
        System.out.println(thisMth);
        System.out.println(lastMth);

        query(model,lastMth,thisMth,lastTime,thisTime,2,2);
    }


    //时间轴为过去24周的周数据
    public void weekLine(String model,String thisTime) throws SQLException, ParseException {
        //根据开始时间推出24周前的时间
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(Date.valueOf(thisTime));

        int day = calendar.get(Calendar.DAY_OF_WEEK);//这周的第几天
        calendar.add(Calendar.DATE,-day);           //上滚N天
        String WeekLastDay = new Date(calendar.getTime().getTime()).toString();//上周的最后一天时间
        int WeekLastDayMth = Utils.disMonth(calendar);   //上周的最后一天对应的mth编号

        calendar.add(Calendar.DATE,-168);
        String Before24WeekDay = new Date(calendar.getTime().getTime()).toString();//24周前的第一天
        int Before24WeekDayMth = Utils.disMonth(calendar);            //24周前的第一天对应的mth编号

        query(model,Before24WeekDayMth,WeekLastDayMth,Before24WeekDay,WeekLastDay,3,6);
    }

    //时间轴为过去24个月的月数据
    public void monthLine(String model, String thisTime) throws ParseException, SQLException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(Date.valueOf(thisTime));

        int thisMth = Utils.disMonth(calendar) - 1;   //这个月的编号
        int lastMth = thisMth - 24;            //前24个月的编号
        query(model,lastMth,thisMth,"","",4,1);
    }


    public void query(String model,int lastMth,int thisMth,String lastTime,String thisTime, int type,int Num) throws SQLException {
        ds = new HiveDataBaseConnection();
        con = ds.getConnection();
        System.out.println(lastMth + "  " + thisMth);
        StringBuffer sb = new StringBuffer();

        String s1 = "";
        String s2 = "";
        String s3 = "";
        if(Num == 2 || Num == 6 || Num == 0){
       s1 = "WITH wtmp AS( "+
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
                    "model, "+
                    "time, "+
                    "fm, "+
            "count(DISTINCT pid) AS pid "+
            "FROM "+
                    "(SELECT g1, "+
                            "sid, "+
                            "cbrdint3 AS model, "+
                            "fm, "+
                            "base64(substr(rowkey,0,10)) AS pid "+
                            "FROM bca LATERAL VIEW explode(brdint3) tbrdint3 AS cbrdint3 "+
                            "WHERE (s3a==1 "+
                                    "OR s9!=1) "+
                            "AND cbrdint3=? "+
                            "AND (mth>=? "+
                                    "AND mth<=?)) t3 "+
            "JOIN wtmp "+
            "ON base64(t3.sid)=base64(substr(wtmp.rowkey,0,16)) "+
            "GROUP BY model, "+
                    "fm, "+
                    "time) ";


            if(Num == 2 || Num == 6){
                s2 = ",all_tmp AS ( "+
                        "SELECT sum(pid) AS pid, "+
                        "sum(g1_cot) AS g1_cot, "+
                        "time, "+
                        "model FROM fm_tmp GROUP BY time,model)  "+
                "SELECT fm2_tmp.model, "+
                        "fm2_tmp.time, "+
                        "all_cot, "+
                        "fm2_cot, "+
                        "g1_cot, "+
                        "((fm2_cot/all_cot)*100) AS percents "+
                "FROM "+
                        "(SELECT model, "+
                                "time, "+
                                "sum(pid) over(ORDER BY fm_tmp.time desc rows between CURRENT row and "+Num+" following)AS fm2_cot "+
                                "FROM fm_tmp "+
                                "WHERE fm=2) fm2_tmp   "+
                        "JOIN "+
                "(SELECT time, "+
                        "sum(pid) over(ORDER BY all_tmp.time desc rows between CURRENT row and 2 following) AS all_cot, "+
                "sum(g1_cot) over(ORDER BY all_tmp.time desc rows between CURRENT row and 2 following) AS g1_cot  "+
                "FROM all_tmp) all_tmp1 "+
                "ON (all_tmp1.time=fm2_tmp.time)";
            }else{  //Num == 0
                s2 = "SELECT SUM(fm_tmp.g1_cot) AS g1_cot, "+
                        "fm2_tmp.model, "+
                        "fm2_tmp.time, "+
                        "((SUM(fm2_tmp.pid_cot)/SUM(fm_tmp.pid_cot))*100) AS percents "+
                        "FROM fm_tmp fm2_tmp "+
                        "JOIN fm_tmp "+
                        "ON(fm_tmp.time=fm2_tmp.time) "+
                        "WHERE fm2_tmp.fm=2 "+
                        "GROUP BY fm2_tmp.model, "+
                        "fm2_tmp.time ";
            }

        }
        if(Num == 1){
            System.out.println(11);
            s1 = "WITH fm_tmp AS (  "+
            "SELECT count(DISTINCT g1) AS g1_cot, "+
            "count(DISTINCT pid) AS pid, "+
            "mth AS time, "+
                    "model, "+
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
                            "AND mth<=? "+
                            "AND cbrdint3=?) t1 "+
            "GROUP BY model,fm,mth) "+
            "SELECT fm2_tmp.model, "+
                    "fm2_tmp.time, "+
                    "g1_cot, "+
                    "((fm2_cot/all_cot)*100) AS percents "+
            "FROM "+
                    "(SELECT sum(pid) AS all_cot, "+
                            "sum(g1_cot) AS g1_cot, "+
                            "time,model FROM fm_tmp "+
                            "GROUP BY time, "+
                            "model) t1 "+
                    "JOIN "+
            "(SELECT sum(pid) AS fm2_cot, "+
                    "time, "+
                    "model "+
            "FROM fm_tmp "+
            "WHERE fm=2 "+
            "GROUP BY time, "+
                    "model) fm2_tmp "+
            "ON (t1.time=fm2_tmp.time) ";
        }

        String hql = s1 + s2 + s3;

        System.out.println(hql);
        pstm = con.prepareStatement(hql);
        if(Num == 0 || Num == 2 || Num == 6){
            pstm.setInt(1,lastMth);
            pstm.setInt(2,thisMth);
            pstm.setString(3,lastTime);
            pstm.setString(4,thisTime);
            pstm.setString(5,model);
            pstm.setInt(6,lastMth);
            pstm.setInt(7,thisMth);
        }else if(Num == 1){
            pstm.setInt(1,lastMth);
            pstm.setInt(2,thisMth);
            pstm.setString(3,model);
        }
        ResultSet rs = pstm.executeQuery();
        List<HiveBean> hiveBean = new ArrayList();
        int i = 1;
        while(rs.next()){
            if(Num == 0 || Num == 1 || Num ==2 || (Num == 6 && (i == 1 || i % 7 == 0))) {
                HiveBean hb = new HiveBean();
                hb.setModel(rs.getString("model"));
                hb.setTime(rs.getString("time"));
                hb.setCot(rs.getInt("g1_cot"));
                hb.setPercent(rs.getDouble("percents"));
                hb.setType(type);
                hiveBean.add(hb);
            }
            i ++;
        }

        //传输数据
        Utils.transData(hiveBean);
        pstm.close();
        con.close();
    }

    public static void main(String[] args) throws SQLException, IOException, ParseException {
        Brd3Evaluate brd3Evaluate = new Brd3Evaluate();
        //source.dayLine("162","2016-04-01");
        //brd3Evaluate.threeDayLine("162","2016-04-04");
        //h.weekLine("162","2016-04-04");
        brd3Evaluate.monthLine("162","2016-04-04");

    }
}
