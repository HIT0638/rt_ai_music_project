package com.music.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.music.bean.UserLoginLocationInfo;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;

public class AmapGeoApiUtil {

    public static final String REVERSE_GEOCODE_URL =
        "https://restapi.amap.com/v3/geocode/regeo";

    private AmapGeoApiUtil() {

    }

    public static String buildLocation(UserLoginLocationInfo value) {
        return value.getLng() + "," + value.getLat();
    }

    public static String buildBatchLocations(List<UserLoginLocationInfo> batch) {
        // TODO: 将 batch 中的经纬度拼成
        // lng1,lat1|lng2,lat2|...
        StringBuilder batches = new StringBuilder();
        for (UserLoginLocationInfo value : batch) {
            batches.append(buildLocation(value)).append("|");
        }
        batches.delete(batches.length() - 1, batches.length());
        return batches.toString();
    }

    public static String buildReverseGeocodeUrl(String apiKey, String location) {
        try {
            return REVERSE_GEOCODE_URL
                + "?key=" + encode(apiKey)
                + "&location=" + encode(location)
                + "&extensions=base";
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("构造高德逆地理编码URL失败", e);
        }
    }

    public static String buildBatchReverseGeocodeUrl(String apiKey, String locations) {
        // TODO: 构造 batch=true 的高德批量逆地理编码 URL
        try {
            return REVERSE_GEOCODE_URL
                    + "?key=" + encode(apiKey)
                    + "&location=" + encode(locations)
                    + "&batch=true"
                    + "&extensions=base"
                    + "&output=json";
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("构造高德逆地理编码URL失败", e);
        }

    }

    public static JSONObject parseResponse(String responseBody) {
        return JSON.parseObject(responseBody);
    }

    public static boolean isSuccess(JSONObject response) {
        // TODO: 根据 status / infocode 判断请求是否成功
        String status = response.getString("status");
        String infocode = response.getString("infocode");
        if ("1".equals(status) && "10000".equals(infocode)) {
            return true;
        }
        return false;
    }

    public static JSONArray extractRegeocodes(JSONObject response) {
        // TODO: 从批量响应中提取 regeocodes 数组
        JSONArray regeocodes = response.getJSONArray("regeocodes");
        return regeocodes;
    }

    public static String extractProvince(JSONObject response) {
        return extractAddressComponent(response, "province");
    }

    public static String extractCity(JSONObject response) {
        return extractAddressComponent(response, "city");
    }

    public static String extractDistrict(JSONObject response) {
        return extractAddressComponent(response, "district");
    }

    public static String extractAdcode(JSONObject response) {
        return extractAddressComponent(response, "adcode");
    }

    public static String extractProvinceFromRegeocode(JSONObject regeocode) {
        // TODO: 从单条 regeocode 中提取 province

        String province = regeocode.getJSONObject("addressComponent").getString("province");
        String province_clean = province.replaceAll("\\s+", "");
        return  province_clean;
    }

    public static String extractCityFromRegeocode(JSONObject regeocode) {
        // TODO: 从单条 regeocode 中提取 city

        String city = regeocode.getJSONObject("addressComponent").getString("city");
        if  (city == null || city.isBlank() || "[]".equals(city)) {
            return regeocode.getJSONObject("addressComponent").getString("province");
        } else {
            return city;
        }
    }

    public static String extractDistrictFromRegeocode(JSONObject regeocode) {
        // TODO: 从单条 regeocode 中提取 district

        return regeocode.getJSONObject("addressComponent").getString("district");
    }

    public static String extractAdcodeFromRegeocode(JSONObject regeocode) {
        // TODO: 从单条 regeocode 中提取 adcode

        return  regeocode.getJSONObject("addressComponent").getString("adcode");
    }

    private static String extractAddressComponent(JSONObject response, String fieldName) {
        // TODO: 根据高德返回结构补完整的空值判断和字段兼容逻辑
        JSONObject regeocode = response.getJSONObject("regeocode");
        if (regeocode == null) {
            return null;
        }
        JSONObject addressComponent = regeocode.getJSONObject("addressComponent");
        if (addressComponent == null) {
            return null;
        }
        return addressComponent.getString(fieldName);
    }

    private static String encode(String value) throws UnsupportedEncodingException {
        return URLEncoder.encode(value, "UTF-8");
    }
}
