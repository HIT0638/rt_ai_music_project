package com.music.bean;

import java.io.Serializable;

public class MachineConsumeWideRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long id;
    private Long mid;
    private Long uid;
    private Long amount;
    private Integer pkgId;
    private String pkgName;
    private Integer activityId;
    private String activityName;
    private String actionTime;
    private String billDate;
    private String province;
    private String city;
    private String mapClass;
    private String sceneAddress;
    private Double invRate;
    private Double ageRate;
    private Double comRate;
    private Double parRate;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getMid() {
        return mid;
    }

    public void setMid(Long mid) {
        this.mid = mid;
    }

    public Long getUid() {
        return uid;
    }

    public void setUid(Long uid) {
        this.uid = uid;
    }

    public Long getAmount() {
        return amount;
    }

    public void setAmount(Long amount) {
        this.amount = amount;
    }

    public Integer getPkgId() {
        return pkgId;
    }

    public void setPkgId(Integer pkgId) {
        this.pkgId = pkgId;
    }

    public String getPkgName() {
        return pkgName;
    }

    public void setPkgName(String pkgName) {
        this.pkgName = pkgName;
    }

    public Integer getActivityId() {
        return activityId;
    }

    public void setActivityId(Integer activityId) {
        this.activityId = activityId;
    }

    public String getActivityName() {
        return activityName;
    }

    public void setActivityName(String activityName) {
        this.activityName = activityName;
    }

    public String getActionTime() {
        return actionTime;
    }

    public void setActionTime(String actionTime) {
        this.actionTime = actionTime;
    }

    public String getBillDate() {
        return billDate;
    }

    public void setBillDate(String billDate) {
        this.billDate = billDate;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getMapClass() {
        return mapClass;
    }

    public void setMapClass(String mapClass) {
        this.mapClass = mapClass;
    }

    public String getSceneAddress() {
        return sceneAddress;
    }

    public void setSceneAddress(String sceneAddress) {
        this.sceneAddress = sceneAddress;
    }

    public Double getInvRate() {
        return invRate;
    }

    public void setInvRate(Double invRate) {
        this.invRate = invRate;
    }

    public Double getAgeRate() {
        return ageRate;
    }

    public void setAgeRate(Double ageRate) {
        this.ageRate = ageRate;
    }

    public Double getComRate() {
        return comRate;
    }

    public void setComRate(Double comRate) {
        this.comRate = comRate;
    }

    public Double getParRate() {
        return parRate;
    }

    public void setParRate(Double parRate) {
        this.parRate = parRate;
    }
}
