package MovieInfoCrawl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MovieInfoCrawler {

//    private final static WebClient client = new WebClient();
//
//    static{
//        client.getOptions().setJavaScriptEnabled(true);// 默认执行js
//        client.getOptions().setCssEnabled(false);
//        client.setAjaxController(new NicelyResynchronizingAjaxController());
//        client.getOptions().setThrowExceptionOnScriptError(false);
//    }

    private static String[] userAgents = {"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:46.0) Gecko/20100101 Firefox/46.0",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.87 Safari/537.36 OPR/37.0.2178.32",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.57.2 (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2486.0 Safari/537.36 Edge/13.10586",
            "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko",
            "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)",
            "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0)",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 BIDUBrowser/8.3 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36 Core/1.47.277.400 QQBrowser/9.4.7658.400",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 UBrowser/5.6.12150.8 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 Safari/537.36 SE 2.X MetaSr 1.0",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36 TheWorld 7",
            "Mozilla/5.0 (Windows NT 6.1; W…) Gecko/20100101 Firefox/60.0"};

    private final Vector<Proxy> proxies = new Vector<>();
    private final ConcurrentLinkedQueue<String> abandonedList = new ConcurrentLinkedQueue<>();

    private final static ExecutorService es = Executors.newFixedThreadPool(5);

//    private static long lastRefreshTime = System.currentTimeMillis();
//
//    private boolean refreshingProxies = false;

//    private void refreshProxies(){
//        if(refreshingProxies){
//            while (refreshingProxies){
//                try{
//                    Thread.sleep(5000);
//                }catch (InterruptedException e){
//                    e.printStackTrace();
//                    return;
//                }
//            }
//            return;
//        }
//        synchronized (proxies){
//            refreshingProxies = true;
//            while(System.currentTimeMillis() - lastRefreshTime < 15000){
//                try{
//                    Thread.sleep(1000);
//                    System.out.println("waiting until time gap passed...");
//                }catch (InterruptedException e){
//                    e.printStackTrace();
//                }
//            }
//            proxies.clear();
//            lastRefreshTime = System.currentTimeMillis();
//            System.out.println("refreshing proxies...");
//            try{
//                Document doc = Jsoup.connect("http://piping.mogumiao.com/proxy/api/get_ip_bs?appKey=b76cf64b43c74d8c9f247cc9b4194c2d&count=5&expiryDate=0&format=1&newLine=2")
//                        .get();
//                System.out.println(doc.text());
//                JSONObject jsonObject = (JSONObject) JSON.parse(doc.text());
//                List<Map<String,Object>> list = (List<Map<String,Object>>) jsonObject.get("msg");
//                for (Map<String,Object> map : list ) {
//                    String ip = (String)map.get("ip");
//                    int port = Integer.parseInt((String)map.get("port"));
//                    proxies.add(new Pair<>(ip, port));
//                }
//            }catch (IOException e){
//                e.printStackTrace();
//            }finally {
//                refreshingProxies = false;
//            }
//        }
//    }

    private Proxy getProxy() {
        try{
            Document doc = Jsoup.connect("http://proxy.qgvps.com:8085/allocate?Key=RN1WFQZE5QX6H9TM&Num=1&KeepAlive=5")
                    .get();
            //System.out.println(doc.text());
            JSONObject jsonObject = (JSONObject) JSON.parse(doc.text());
            Integer code = jsonObject.getInteger("Code");
            if(code == 0){
                System.out.println("get proxy");
                List<Map<String,Object>> list = (List<Map<String,Object>>) jsonObject.get("Data");
                String ip = (String)list.get(0).get("IP");
                int port = Integer.parseInt((String)list.get(0).get("port"));
                Proxy proxy = new Proxy(ip, port);
                System.out.println("get proxy " + proxy.ip + ":" + proxy.port);
                return new Proxy(ip, port);
            }
            return null;
        }catch (IOException e){
            e.printStackTrace();
            return null;
        }
    }

    private void abandonProxy(String ip){
        abandonedList.offer(ip);
        es.submit(new Runnable() {
            @Override
            public void run() {
                try{
                    while(true){
                        String ip = abandonedList.poll();
                        if(ip == null){
                            Thread.sleep(1000);
                            continue;
                        }
                        while(!abandonProxy(ip)){
                            Thread.sleep(5000);
                        }
                        break;
                    }
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }

            private boolean abandonProxy(String ip){
                try{
                    Document doc = Jsoup.connect("http://proxy.qgvps.com:8085/release?Key=RN1WFQZE5QX6H9TM&IP=" + ip)
                            .get();
                    System.out.println(doc.text());
                    JSONObject jsonObject = (JSONObject) JSON.parse(doc.text());
                    Integer code = jsonObject.getInteger("Code");
                    if(code == 0){
                        return true;
                    }
                    return false;
                }catch (IOException e){
                    e.printStackTrace();
                    return false;
                }
            }
        });
    }

    private Map<String, String> cookies = new HashMap<>();

    private Random random = new Random();

    public Document crawlOneProduct(String productId, boolean useProxy){

          String baseUrl = "https://www.amazon.com/gp/product/";
//        HtmlPage page;
//        synchronized (client){
//            try {
//                page = client.getPage(baseUrl + productId);
//            }catch (Exception e){
//                e.printStackTrace();
//                return null;
//            }
//        }
//        return Jsoup.parse(page.asXml());
        if(!useProxy){
            try{
                Connection.Response response = Jsoup.connect(baseUrl + productId).timeout(30000)
                        .method(Connection.Method.GET)
                        .cookies(cookies)
                        .header("user-agent", userAgents[random.nextInt(userAgents.length)])
                        .execute();
                cookies = response.cookies();
                return response.parse();
            }catch (IOException e){
                //e.printStackTrace();
                return null;
            }
        }
        Proxy proxy;
        try{
            if(proxies.size() < 5){
                Proxy newProxy = getProxy();
                while(newProxy == null){
                    Thread.sleep(1000);
                    System.out.println("waiting for proxies...");
                    newProxy = getProxy();
                }
                synchronized (proxies){
                    proxies.add(newProxy);
                }
            }
            synchronized (proxies){
                proxy = proxies.get(random.nextInt(proxies.size()));
            }
            while(System.currentTimeMillis() - proxy.lastUse.get() < 5000){
                Thread.sleep(1000);
            }
            proxy.lastUse.set(System.currentTimeMillis());
        }catch (InterruptedException e){
            //e.printStackTrace();
            return null;
        }
        try{
            Connection.Response response = Jsoup.connect(baseUrl + productId).timeout(30000)
                    .method(Connection.Method.GET)
                    .cookies(cookies == null ? new HashMap<String, String>() : cookies)
                    .header("user-agent", userAgents[random.nextInt(userAgents.length)])
                    .proxy(proxy.ip, proxy.port)
                    .execute();

            cookies = response.cookies();

            Document doc = response.parse();
            if(!MovieInfoTransformer.checkValidity(doc)){
                throw new IOException("rejected");
            }
            return doc;
        }catch (IOException e){
            //e.printStackTrace();
            if(proxy.decreaseAndGetRetryTime() == 0){
                System.out.println("abandoning proxy ip " + proxy.ip);
                abandonProxy(proxy.ip);
                synchronized (proxies){
                    proxies.remove(proxy);
                }
            }
            return null;
        }
    }

    public Document crawlOneProduct(String productId){
        return crawlOneProduct(productId, true);
    }


    public static void main(String[] args) {
        MovieInfoCrawler crawler = new MovieInfoCrawler();
        Document doc = crawler.crawlOneProduct("0001489305");
        System.out.println(doc);
    }

    static class Proxy {
        String ip;
        int port;
        AtomicInteger retryTime = new AtomicInteger(3);
        AtomicLong lastUse = new AtomicLong(System.currentTimeMillis());
        Proxy(String ip, int port) {
            this.ip = ip;
            this.port = port;
        }

        public int getRetryTime() {
            return retryTime.get();
        }

        public int decreaseAndGetRetryTime() {
            return this.retryTime.decrementAndGet();
        }
    }

}
