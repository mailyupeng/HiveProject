package com.mobin.Hive.KPI;

/**
 * Created by MOBIN on 2016/9/22.
 */
public class HiveBean {
    private String model;
    private String time;
    private int brd3_cot;
    private int g1_cot;
    private String s3a_attimg;
    private double per;
    private double per1;
    private int type;

    public HiveBean() {

    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public int getBrd3_cot() {
        return brd3_cot;
    }

    public void setBrd3_cot(int brd3_cot) {
        this.brd3_cot = brd3_cot;
    }

    public int getG1_cot() {
        return g1_cot;
    }

    public void setG1_cot(int g1_cot) {
        this.g1_cot = g1_cot;
    }

    public String getS3a_attimg() {
        return s3a_attimg;
    }

    public void setS3a_attimg(String s3a_attimg) {
        this.s3a_attimg = s3a_attimg;
    }

    public double getPer() {
        return per;
    }

    public void setPer(double per) {
        this.per = per;
    }

    public double getPer1() {
        return per1;
    }

    public void setPer1(double per1) {
        this.per1 = per1;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }
}
