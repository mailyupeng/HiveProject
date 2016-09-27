package com.mobin.Hive;

/**
 * Created by MOBIN on 2016/9/22.
 */
public class HiveBean {
    private String model;//车系
    private String time;
    private int cot;
    private int type;
    private double percent;

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

    public int getCot() {
        return cot;
    }

    public void setCot(int cot) {
        this.cot = cot;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public double getPercent() {
        return percent;
    }

    public void setPercent(double percent) {
        this.percent = percent;
    }
}
