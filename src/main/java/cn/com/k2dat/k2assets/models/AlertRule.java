package cn.com.k2dat.k2assets.models;

public class AlertRule {

    public Integer id;
    public String name;
    public String measurement;
    public String thresholds;

    public AlertRule(Integer id, String name, String measurement, String thresholds) {
        this.id = id;
        this.name = name;
        this.measurement = measurement;
        this.thresholds = thresholds;
    }

    public AlertRule() {
    }


}
