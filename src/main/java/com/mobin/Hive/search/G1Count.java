package com.mobin.Hive.search;

import com.mobin.Hive.KPI.HiveDataBaseConnection;
import com.mobin.Hive.Utils.Utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by MOBIN on 2016/10/28.
 */
public class G1Count {
    private static HiveDataBaseConnection ds;
    private static Connection con;
    private static PreparedStatement pstm;

    static class CountBean{
        private String brdint3;
        private String cot;

        public String getBrdint3() {
            return brdint3;
        }

        public void setBrdint3(String brdint3) {
            this.brdint3 = brdint3;
        }

        public String getCot() {
            return cot;
        }

        public void setCot(String cot) {
            this.cot = cot;
        }
    }

    public void query(String conditins){
        ds = new HiveDataBaseConnection();
        con = ds.getConnection();
        String hql ="";
        //JDK7异常处理特性无需显示关闭IO
        String path = "HQLFile\\搜索\\用户导向统计.sql";
        try(FileInputStream in = new FileInputStream(path)){
            byte[] bytes = new byte[in.available()];
            in.read(bytes);
            hql = new String(bytes).replaceAll("\\$\\{hivevar:num\\}",conditins); //三/七天线中的滑动窗口单位
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<CountBean> countBeens = null;
        try {
            System.out.println(hql);
            pstm = con.prepareStatement(hql);
            ResultSet rs = pstm.executeQuery();
            countBeens = new ArrayList<>();
            while (rs.next()){
                CountBean countBean = new CountBean();
                countBean.setBrdint3(rs.getString("cbrdint3"));
                countBean.setCot(rs.getString("cot"));
                countBeens.add(countBean);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
         //0仅仅作为方法的补充参数
        Utils.transData(countBeens,2,0);

    }
}
