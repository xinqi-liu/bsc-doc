package monitorUtils;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Arrays;
import java.lang.Float;
import java.lang.Float;
public class ApiAlertUtilsAbnormal {
    
     //预处理计算函数calFunc所需数据和符号,返回ApiAlertUtilsCalModel
    public static ApiAlertUtilsCalModel preProcess(
        ApiAlertUtilsApiIdModel tempModel,
        String indicatorSet,
        String codeSet) 
    {
        ApiAlertUtilsCalModel preProcessLabel = new ApiAlertUtilsCalModel();
        String func;
        if(indicatorSet == "pvCount") {
            preProcessLabel.indicator1 = tempModel.pvCountTotal;
            preProcessLabel.indicator2 = null;
            preProcessLabel.func = null;
        }
        else if (indicatorSet == "requestTime") {
            preProcessLabel.indicator1 = tempModel.sumElapse;
            preProcessLabel.indicator2 = tempModel.pvCountTotal;
            preProcessLabel.func = "/";
        }
        else if (indicatorSet == "httpCode") {
            if (tempModel.httpCodeMap.containsKey(codeSet)){
                preProcessLabel.indicator1 = tempModel.httpCodeMap.get(codeSet);
            }
            else {
                preProcessLabel.indicator1 = 0f;
            }
            preProcessLabel.indicator2 = tempModel.pvCountTotal;
            preProcessLabel.func = "/";
        }
        return preProcessLabel;
    }
     //计算函数，返回bool值,true则报警，false则不报警
    public static boolean calFunc(ApiAlertUtilsCalModel preProcessLabel,String compare,Float number) {
        boolean bool = false;
        Float indicator1 = preProcessLabel.indicator1;
        Float indicator2 = preProcessLabel.indicator2;
        String func = preProcessLabel.func;
        Float result;
        if(func == "+"){
            result = indicator1+indicator2;
        }
        else if(func == "-") {
            result = indicator1-indicator2;
        }
        else if(func == "*"){
            result = indicator1*indicator2;
        }
        else if(func == "/"){
            result = indicator1/indicator2;
        }
        else {
            result = indicator1;
        }
        if (compare == ">"){
            if(result>number) {
                bool = true;
            }
        }
        else if (compare == "<"){
            if(result<number) {
                bool = true;
            }
        }
        else if (compare == "="){
            if(result==number) {
                bool = true;
            }
        }
        return bool;
    }
    
    //按照apiId,period聚合每分钟数据，返回tempModel
    public static ApiAlertUtilsApiIdModel UtilsGroupListAccumulate(
        ArrayList<Map<String,ApiAlertUtilsApiIdModel>> groupList,
        String apiId,
        String period)
    {
        ApiAlertUtilsApiIdModel GroupListAccumulate;
        Float pvCountTotal = 0f; 
        Float sumElapse = 0.0f;
        Float pvCount = 0f; 
        ApiAlertUtilsApiIdModel tempModel = new ApiAlertUtilsApiIdModel();
        
        for(int i=0; i<Float.parseFloat(period)/60; i++) {
            if(groupList.contains(i)) {
        		if(groupList.get(i).containsKey(apiId)) {
                    pvCountTotal = pvCountTotal + groupList.get(i).get(apiId).pvCountTotal;
                    sumElapse = sumElapse + groupList.get(i).get(apiId).sumElapse;
                    Map<String, Float> map = new HashMap<String, Float>();
                    for(Map.Entry<String, Float> entry : groupList.get(i).get(apiId).httpCodeMap.entrySet()){
                        String httpCode = entry.getKey();
                        if(tempModel.httpCodeMap.containsKey(httpCode)) {
                            Float temp = tempModel.httpCodeMap.get(httpCode) + entry.getValue();
                            tempModel.httpCodeMap.put(httpCode,temp);
                        }
                        else {
                            tempModel.httpCodeMap.put(httpCode,entry.getValue());
                        }   
                    }
                }
            }
        }
        return tempModel;
    }
    
    //如果触发报警则返回相应的ArrayList，否则为null
    public static Map<String,String> AlertOrNot(ApiAlertUtilsConfigModel config) 
    {
        Map<String,String> abnormalMsg = new HashMap<String,String>();
        abnormalMsg.put("apiId",config.apiId);
        abnormalMsg.put("indicatorId",config.indicatorId);
        abnormalMsg.put("strategyId",config.strategyId);
        //如果触发报警则返回相应的ArrayList，否则为null
        abnormalMsg.put("time",String.valueOf(System.currentTimeMillis()));
        return abnormalMsg;
    }
    //处理函数
    public static ArrayList<Map<String,String>> UtilsAbnormal(
        ArrayList<Map<String,ApiAlertUtilsApiIdModel>> groupList,
        ArrayList<ApiAlertUtilsConfigModel> configs)
    {
        ArrayList<Map<String,String>> abnormalList = new ArrayList<Map<String,String>>();
        //遍历每条报警
        for(int i=0;i<configs.size();i++) {
            ApiAlertUtilsConfigModel config = configs.get(i);
            int label = 0;
            String apiId = config.apiId;
            String period = config.period;
            ApiAlertUtilsApiIdModel tempModel = UtilsGroupListAccumulate(groupList,apiId,period);
            //遍历每条报警的报警条件，并关系：所有条件满足则报警
            for(int j=0;j<config.content.size();j++) {
                String indicatorSet = config.content.get(j).get("indicatorSet");
                String codeSet = config.content.get(j).get("codeSet");
                Float number = Float.parseFloat(config.content.get(j).get("number"));
                String compare = config.content.get(j).get("compare");
                ApiAlertUtilsCalModel preProcessLabel = preProcess(tempModel,indicatorSet,codeSet);
                //判断是否需要报警
                boolean bool = calFunc(preProcessLabel,compare,number);
                if(bool == true){
                    label=label+1;
                }
            }
            //如果满足报警条件则将abnormalMsg添加至abnormalList
            if (label == config.content.size()) {
                Map<String,String> abnormalMsg= AlertOrNot(config);
                abnormalList.add(abnormalMsg);
            }
            
        }
        return abnormalList;
    }
    
}
