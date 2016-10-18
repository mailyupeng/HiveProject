package com.mobin.Hive;

import com.mobin.Hive.Utils.Utils;

import java.io.IOException;
import java.sql.Date;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Calendar;

/**
 * Created by MOBIN on 2016/10/10.
 */
 /*
        query函数的最后个参数
      * 日线：0
      * 三天线：2（三天线需要用到Hive中的窗口函数，窗口函数中的滑动单位为2）
      * 周线：6（月线需要用到Hive中的窗口函数，窗口函数中的滑动单位为6）
      * 月线：1
      * */
public class Common {
    /**（日线）每天关注该车系的人数,时间轴为30天
     * thisMth:根据时间获得对应的mth的编号
     * lastMth:上个月的mth编号
     * lastTime:上个月的时间
     * @param startTime:开始时间
     * @param type:表示X线，如：日线 月线
     * */
    public void dayLine(String startTime, int type) throws SQLException, IOException, ParseException {
        //根据开始时间推出上个月的时间
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(Date.valueOf(startTime));

        int thisMth = Utils.disMonth(calendar);
        int lastMth = thisMth - 1;

        calendar.roll(Calendar.MONTH, false);   //上滚一个月
        String lastTime = new Date(calendar.getTime().getTime()).toString();

        query(lastMth, thisMth,lastTime, startTime, type, 0);  //0表示日线
    }

    /**（三天线）
    * thisMth:今天对应的mth编号
    * lastMth:三天前的mth编号
    * lastTime:三天前的时间
     * @param startTime:开始时间
     * @param type:表示X线，如：日线 月线
     * */
    public void threeDayLine(String startTime, int type) throws ParseException, SQLException {
        //根据开始时间推出三天前的时间
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(Date.valueOf(startTime));
        int thisMth = Utils.disMonth(calendar);

        calendar.add(Calendar.DATE, -32);  //根据时间减去(-)/添加时间量，该方法表示在当前时间下减去32天
        String lastTime = new Date(calendar.getTime().getTime()).toString();
        int lastMth = Utils.disMonth(calendar);
        System.out.println(thisMth);
        System.out.println(lastMth);

        query(lastMth, thisMth,lastTime, startTime, type, 2);//2表示三天线
    }

    /**（周线）
     * day:计算当前时间为这周的第几天
     * thisMth:今天对应的mth编号
     * WeekLastDayMth:上周的最后一天对应的mth编号
     * WeekLastDay:上周的最后一天时间
     * Before24WeekDay：24周前的第一天
     * Before24WeekDayMth：24周前的第一天对应的mth编号
     * @param startTime:开始时间
     * @param type:表示X线，如：日线 月线
     * */
    //时间轴为过去24周的周数据
    public void weekLine(String startTime, int type) throws SQLException, ParseException {
        //根据开始时间推出24周前的时间
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(Date.valueOf(startTime));

        int day = calendar.get(Calendar.DAY_OF_WEEK);
        calendar.add(Calendar.DATE, -day);           //上滚N天到上周的最后一天
        String WeekLastDay = new Date(calendar.getTime().getTime()).toString();
        int WeekLastDayMth = Utils.disMonth(calendar);

        calendar.add(Calendar.DATE, -168);
        String Before24WeekDay = new Date(calendar.getTime().getTime()).toString();
        int Before24WeekDayMth = Utils.disMonth(calendar);

        query(Before24WeekDayMth, WeekLastDayMth, Before24WeekDay, WeekLastDay, type, 6);  //3表示周线
    }

    /**（三天线）
     * thisMth:这个月对应的mth编号
     * lastMth:前24个月对应的mth编号
     * lastTime:三天前的时间
     * @param startTime:开始时间
     * @param type:表示X线，如：日线 月线
     * */
    //时间轴为过去24个月的月数据
    public void monthLine(String startTime, int type) throws ParseException, SQLException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(Date.valueOf(startTime));

        int thisMth = Utils.disMonth(calendar) - 1;
        int lastMth = thisMth - 24;
        query(lastMth, thisMth, "", "", type, 1);   //1表示月线
    }

    /*
    * 1:评论声量和声量份额
    * 3:声量来源
    * 4:整车质量和产品性能评价质量评价
    * 6:品牌总体评价
    * 7:品牌形象指标评价
    * */
    public void query(int lastMth, int thisMth,String lastTime,String startTime, int type, int Num) throws SQLException {
         if(type == 1){
             Comment.query(lastMth, thisMth, lastTime, startTime, type, Num);
         }else if(type == 3){
             Source.query(lastMth, thisMth, lastTime, startTime, type, Num);
         }else if(type == 4){
             QualityEvalute.query(lastMth, thisMth, lastTime, startTime, type, Num);
         }else if(type == 6){
             Brd3Evaluate.query(lastMth, thisMth, lastTime, startTime, type, Num);
         }else{//key==7
             BrandEvaluate.query(lastMth, thisMth, lastTime, startTime, type, Num);
         }
    }
    //脚本输入参数为时间(格式:yyyy-MM-dd)和X线编号
    public static void main(String[] args) throws ParseException, SQLException, IOException {
        Common common = new Common();
//        int[] types = new int[]{1,3,4,6,7};
//        if(args[1].equals("1")){       //日线
//            for (int type : types) {
//                common.dayLine(args[0],type);
//            }
//        }else if(args[1].equals("2")){  //三天线
//            for (int type : types) {
//                common.threeDayLine(args[0],type);
//            }
//        }else if(args[1].equals("3")){   //周线
//            for (int type : types) {
//                common.weekLine(args[0],type);
//            }
//        }else if(args[1].equals("4")){   //月线
//            for (int type : types) {
//                common.monthLine(args[0],type);
//            }
//        }else {
//            System.err.println("请输入正确的参数！");
//            return;
//        }
       // common.monthLine("2016-04-01",3);
        System.out.println(args[0] + "  " + args[1]);
        //common.dayLine("2016-04-01",3);
       // common.dayLine("2016-04-01",4);
        common.dayLine("2016-04-01",4);
      //  common.threeDayLine("2016-04-01",4);
    }
}