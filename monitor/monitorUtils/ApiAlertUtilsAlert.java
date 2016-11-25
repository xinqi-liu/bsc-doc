package monitorUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Iterator;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.JsonNode ;
import com.mashape.unirest.http.HttpResponse;
//引入slf4j接口的Logger和LoggerFactory
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//发送报警
public class ApiAlertUtilsAlert {
    final static Logger logger = LoggerFactory.getLogger(ApiAlertUtilsConfigParse.class);
    ArrayList<Map<String,String>> abnormalList;
    //http://10.211.55.4:8002/internal/alert
    String postUrl;
    public static void UtilsAlert(ArrayList<Map<String,String>> abnormalList,String postUrl) {
        Iterator<Map<String,String>> it = abnormalList.iterator();
        while(it.hasNext()) {
            Map<String,String>  abnormal = it.next();
            String body = "{  \"api_id\": \""+abnormal.get("apiId")+"\",\n  \"strategy_id\": \""+abnormal.get("strategyId")+"\",  \"indicator_id\": \""+abnormal.get("indicatorId")+"\",\n  \"time\": \""+abnormal.get("time")+"\"}";
            System.out.println("alert message: " + body) ;
            JsonNode bodyJson = new JsonNode(body) ;
            try{
                HttpResponse<JsonNode> response = Unirest.post(postUrl)
                    .header("content-type", "application/json")
                    .body(bodyJson)
                    .asJson();
            }catch(Exception e){
                String errorMessage=String.format("ApiAlertUtilsAlert:Error while send alert to %s",postUrl);
                logger.error(errorMessage,e);
            }
        }
    }
}
