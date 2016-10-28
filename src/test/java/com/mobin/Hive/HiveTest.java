package com.mobin.Hive;
import com.mobin.Hive.KPI.Common;
import com.mobin.Hive.KPI.HiveBean;
import com.mobin.Hive.KPI.HiveDataBaseConnection;
import com.mobin.Hive.Utils.Utils;
import com.mobin.Hive.recommend.G1Recommend;
import com.mobin.Hive.recommend.PostsRecommend;
import com.mobin.Hive.recommend.RecommendBean;
import org.junit.Test;


import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;


public class HiveTest {
    private static HiveDataBaseConnection ds;
    private static Connection con;
    private static PreparedStatement pstm;
    private static String tableName = "bca";
    private static String subtableName = "raws";

    @Test
    public void jdbc() throws ParseException, SQLException, IOException {
        Common common = new Common();
//        int[] types = new int[]{1,3,4,6,7};
//        if("1".equals("1")) {       //日线
//            for (int type : types)
//
//        }
//        List<HiveBean> hiveBean = new ArrayList();
//        HiveBean hb = new HiveBean();
//        hb.setModel("mol");
//        hb.setTime("2013-01-01");
//        hb.setBrd3_cot(3);
//        hb.setG1_cot(1);
//        hb.setPer(23.2);
//        hb.setType(3);
//        hiveBean.add(hb);
//        Utils.transData(hiveBean);
        common.dayLine("2016-04-01");

    }


    @Test
    public void recommend() {
        //文章推荐
        PostsRecommend.query(2, 61, "b2", "", "2015-04-30", 20);
        //用户推荐
        G1Recommend.query("2016-01-01", 1);
    }
}
