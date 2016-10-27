package com.mobin.Hive.KPI;

import org.apache.hadoop.hive.ql.metadata.Hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by MOBIN on 2016/9/21.
 */
public class HiveDataBaseConnection {
    private final static String DriverName = "org.apache.hive.jdbc.HiveDriver";
    private final static String URL = "jdbc:hive2://master36:10003/default";
    private final static String UserName = "hadoop";
    private final static String Password = "";  //默认为空
    private Connection con;

    public HiveDataBaseConnection(){
        try {
            Class.forName(DriverName);
            con = DriverManager.getConnection(URL,UserName, Password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Connection getConnection(){
        return con;
    }

    public void close(){
            try {
                if(con != null)
                  con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
    }
}
