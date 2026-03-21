package com.music.function;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.music.bean.UserLoginLocationInfo;
import com.music.bean.UserLoginLocationWideRecord;
import com.music.util.AmapGeoApiUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;

public class UserLoginLocationBatchEnrichFunction
    extends RichFlatMapFunction<UserLoginLocationInfo, UserLoginLocationWideRecord> {

    private final int batchSize;
    private final String amapApiKey;
    private transient List<UserLoginLocationInfo> buffer;

    public UserLoginLocationBatchEnrichFunction(int batchSize, String amapApiKey) {
        this.batchSize = batchSize;
        this.amapApiKey = amapApiKey;
    }

    @Override
    public void open(Configuration parameters) {
        buffer = new ArrayList<>(batchSize);
    }

    @Override
    public void flatMap(
        UserLoginLocationInfo value,
        Collector<UserLoginLocationWideRecord> out) throws Exception {

        buffer.add(value);
        if (buffer.size() >= batchSize) {
            flushBuffer(out);
        }
    }

    @Override
    public void close() throws Exception {
        if (buffer != null && !buffer.isEmpty()) {
            // TODO: close() 阶段无法继续可靠地向下游 collect。
            // 如果后面要保证尾批数据不丢，需要改成定时 flush 或 ProcessFunction + Timer。
            buffer.clear();
        }
    }

    private void flushBuffer(Collector<UserLoginLocationWideRecord> out) throws Exception {
        List<UserLoginLocationInfo> batch = new ArrayList<>(buffer);
        buffer.clear();

        if (batch.isEmpty() || amapApiKey == null || amapApiKey.isEmpty()) {
            return;
        }

        String locations = AmapGeoApiUtil.buildBatchLocations(batch);
        if (locations == null || locations.isEmpty()) {
            return;
        }

        String url = AmapGeoApiUtil.buildBatchReverseGeocodeUrl(amapApiKey, locations);

        if (url == null || url.isEmpty()) {
            return;
        }

        // TODO 1: 使用你选择的 HTTP 客户端发送 GET 请求
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(url))
                .GET()
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        JSONObject res = AmapGeoApiUtil.parseResponse(response.body());
        JSONArray regeoCodes = null;

        if (AmapGeoApiUtil.isSuccess(res)) {
            regeoCodes = AmapGeoApiUtil.extractRegeocodes(res);
            if (regeoCodes == null || regeoCodes.size() == 0) {
                throw new RuntimeException("获取高德地理信息失败");
            }
        } else {
            throw new RuntimeException("获取高德地理信息失败");
        }


        for  (int i = 0; i < Math.min(batch.size(), regeoCodes.size()); i++) {
            JSONObject regeoCode = regeoCodes.getJSONObject(i);
            UserLoginLocationInfo userLoginLocationInfo = batch.get(i);
            UserLoginLocationWideRecord userLoginLocationWideRecord = new UserLoginLocationWideRecord();
            userLoginLocationWideRecord.setUserId(userLoginLocationInfo.getUserId());
            userLoginLocationWideRecord.setLng(userLoginLocationInfo.getLng());
            userLoginLocationWideRecord.setLat(userLoginLocationInfo.getLat());
            userLoginLocationWideRecord.setLoginDt(userLoginLocationInfo.getLoginDt());
            userLoginLocationWideRecord.setMachineId(userLoginLocationInfo.getMachineId());
            userLoginLocationWideRecord.setDate(userLoginLocationInfo.getDate());
            userLoginLocationWideRecord.setHour(userLoginLocationInfo.getHour());
            userLoginLocationWideRecord.setProvince(
                AmapGeoApiUtil.extractProvinceFromRegeocode(regeoCode));
            userLoginLocationWideRecord.setCity(
                AmapGeoApiUtil.extractCityFromRegeocode(regeoCode));
            userLoginLocationWideRecord.setDistrict(
                AmapGeoApiUtil.extractDistrictFromRegeocode(regeoCode));
            userLoginLocationWideRecord.setAdcode(
                AmapGeoApiUtil.extractAdcodeFromRegeocode(regeoCode));

            out.collect(userLoginLocationWideRecord);
        }

        // TODO 2: 用 AmapGeoApiUtil.parseResponse(...) 解析 JSON 响应
        // TODO 3: 用 AmapGeoApiUtil.isSuccess(...) 判断请求是否成功
        // TODO 4: 提取 regeocodes，并按索引与 batch 中的原始记录一一对应
        // TODO 5: 构造 UserLoginLocationWideRecord，并回填 province/city/district/adcode
        // TODO 6: 如果高德返回条数和 batch 条数不一致，再补重试或补偿逻辑


    }
}
