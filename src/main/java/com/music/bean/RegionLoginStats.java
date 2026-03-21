package com.music.bean;

import java.io.Serializable;

public class RegionLoginStats implements Serializable {
    private static final long serialVersionUID = 1L;

    private String province;
    private String city;
    private String district;
    private String adcode;
    private Long loginUserCount;
    private String windowStart;
    private String windowEnd;

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

    public String getDistrict() {
        return district;
    }

    public void setDistrict(String district) {
        this.district = district;
    }

    public String getAdcode() {
        return adcode;
    }

    public void setAdcode(String adcode) {
        this.adcode = adcode;
    }

    public Long getLoginUserCount() {
        return loginUserCount;
    }

    public void setLoginUserCount(Long loginUserCount) {
        this.loginUserCount = loginUserCount;
    }

    public String getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(String windowStart) {
        this.windowStart = windowStart;
    }

    public String getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(String windowEnd) {
        this.windowEnd = windowEnd;
    }
}
