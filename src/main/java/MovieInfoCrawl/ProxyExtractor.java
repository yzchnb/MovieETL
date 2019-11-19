package MovieInfoCrawl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class ProxyExtractor {
    private static Random random = new Random();
    public Proxy getProxy(){
        try{
            String url = "http://127.0.0.1:5010/get_all/";
            Document doc = Jsoup.connect(url)
                    .ignoreContentType(true)
                    .get();
            List<Map<String, Object>> proxies = (List<Map<String, Object>>) JSON.parse(doc.text());
            if(proxies.size() == 0){
                return new Proxy("127.0.0.1", 1087);
            }else{
                Map<String, Object> randomProxy = proxies.get(random.nextInt(proxies.size()));
                String proxyString = (String)randomProxy.get("proxy");
                Proxy newProxy = new Proxy(proxyString.split(":")[0], Integer.parseInt(proxyString.split(":")[1]));
                System.out.println("get new proxy " + proxyString);
                return newProxy;
            }


        }catch (IOException e){
            e.printStackTrace();
            return new Proxy("127.0.0.1", 1087);
        }
    }

    public void failProxy(Proxy proxy){
        int retryTime = proxy.decreaseAndGetRetryTime();
        if(retryTime <= 0){
            try{
                System.out.println("abandoning proxy " +  proxy.toString());
                String url = "http://127.0.0.1:5010/delete?proxy=" + proxy.toString();
                Document doc = Jsoup.connect(url)
                        .ignoreContentType(true)
                        .get();
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }
}
