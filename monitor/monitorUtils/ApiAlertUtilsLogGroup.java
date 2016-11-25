package monitorUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.lang.Float;
//引入slf4j接口的Logger和LoggerFactory
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//聚合日志
public class ApiAlertUtilsLogGroup {
    ArrayList<Map<String,ApiAlertUtilsApiIdModel>> groupList= new ArrayList<Map<String,ApiAlertUtilsApiIdModel>>();
    Map<String,ApiAlertUtilsApiIdModel> apiMap;
    Map<String,String> log;
    public static Map<String,ApiAlertUtilsApiIdModel> UtilsLogGroup(
        Map<String,ApiAlertUtilsApiIdModel> apiMap,
        Map<String,String> log) 
    {
        ApiAlertUtilsApiIdModel apiIdModel;
        Map<String,Float> httpCodeMap;
        Float pvCount;
        Float sumElapse;
        Float pvCountTotal;
        String apiId = log.get("apiId");
        Float requestTime = Float.parseFloat(log.get("requestTime"));
        String httpCode = log.get("httpCode");
        //判断最后一分钟数据中是否有此apiId
        if(apiMap.containsKey(apiId)) {
            apiIdModel=apiMap.get(apiId);
            httpCodeMap=apiMap.get(apiId).httpCodeMap;
            pvCountTotal = apiMap.get(apiId).pvCountTotal + 1;
            sumElapse =  apiMap.get(apiId).sumElapse + requestTime;
            //判断此apiId数据中是否有此httpCode
            if(apiMap.get(apiId).httpCodeMap.containsKey(httpCode)) {
                pvCount = apiMap.get(apiId).httpCodeMap.get(httpCode) + 1;
            }
            else {
                pvCount = 1f;
            }
        }
        else {
            apiIdModel = new ApiAlertUtilsApiIdModel();
            httpCodeMap = new HashMap<String,Float>();
            pvCount = 1f;
            pvCountTotal = 1f;
            sumElapse = requestTime;
        }
        httpCodeMap.put(httpCode,pvCount);
        apiIdModel.pvCountTotal = pvCountTotal;
        apiIdModel.sumElapse = sumElapse;
        apiIdModel.httpCodeMap = httpCodeMap;
        apiMap.put(apiId,apiIdModel);
        return apiMap;
    }
    public static ArrayList<Map<String,ApiAlertUtilsApiIdModel>> UtilsLogUpdate(
            ArrayList<Map<String,ApiAlertUtilsApiIdModel>> groupList,
            Map<String,ApiAlertUtilsApiIdModel> apiMap,
            Map<String,String> log,
            boolean ifUpdateIndex)
    {
        Map<String,ApiAlertUtilsApiIdModel> apiMapNew = UtilsLogGroup(apiMap,log);
        //groupList每个元素index加1
        if(ifUpdateIndex == true) {
            groupList.addAll(1,groupList);
            //删除index>=30的httpCodeMap
            if(groupList.size()>30) {
                for(int i=30; i<groupList.size(); i++) {
                    groupList.remove(i);
                }
            }   
            //替换第一个元素
            groupList.add(0,apiMapNew);
        }   
        else {
            //更新groupList
            groupList.add(0,apiMapNew);
        }
        return groupList;
    }
    
    
        
}
