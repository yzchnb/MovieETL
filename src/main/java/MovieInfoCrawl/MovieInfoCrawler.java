package MovieInfoCrawl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MovieInfoCrawler {
    private ProxyExtractor proxyExtractor = new ProxyExtractor();

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

    //private final Vector<Proxy> proxies = new Vector<>();
    //private final ConcurrentHashSet<Proxy> proxies = new ConcurrentHashSet<>();
    //private final ConcurrentLinkedQueue<String> abandonedList = new ConcurrentLinkedQueue<>();

    //private final static ExecutorService es = Executors.newFixedThreadPool(5);

    private static long lastRefreshTime = System.currentTimeMillis();
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

//    private void addProxies() {
//        synchronized (proxies){
//            try{
//                while(System.currentTimeMillis() - lastRefreshTime < 15 * 1000){
//                    Thread.sleep(1000);
//                    System.out.println("waiting for time gap passed...");
//                }
//                String url = "http://piping.mogumiao.com/proxy/api/get_ip_bs?appKey=cc6037b2f7204ec396dcfb6ca0a694fc&count=5&expiryDate=0&format=1&newLine=2";
//                Document doc = Jsoup.connect(url)
//                        .get();
//                //System.out.println(doc.text());
//                JSONObject jsonObject = (JSONObject) JSON.parse(doc.text());
//                Integer code = jsonObject.getInteger("code");
//                if(code == 0){
//                    List<Map<String,Object>> list = (List<Map<String,Object>>) jsonObject.get("msg");
//                    for (Map<String, Object> map : list) {
//                        String ip = (String)map.get("ip");
//                        int port = Integer.parseInt((String)map.get("port"));
//                        Proxy proxy = new Proxy(ip, port);
//                        System.out.println("get proxy " + proxy.ip + ":" + proxy.port);
//                        proxies.add(proxy);
//                    }
//                }else{
//                    addProxies();
//                }
//            }catch (IOException | InterruptedException e){
//                e.printStackTrace();
//            }
//        }
//    }

    private Map<String, String> cookies = new HashMap<>();

    private Random random = new Random();

    public Document crawlOneProduct(String productId, boolean useProxy) throws Exception{

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
        Proxy proxy = proxyExtractor.getProxy();
//        try{
//            while(proxy == null){
//                synchronized (proxies){
//                    if(proxies.size() < 5){
//                        addProxies();
//                    }
//                    Iterator<Proxy> proxyIter = proxies.iterator();
//                    int randomIndex = random.nextInt(proxies.size());
//                    for (int i = 0; i < randomIndex; i++) {
//                        proxy = proxyIter.next();
//                    }
//                }
//                if(proxy == null){
//                    continue;
//                }
//                while(System.currentTimeMillis() - proxy.lastUse.get() < 5000){
//                    Thread.sleep(1000);
//                }
//                proxy.lastUse.set(System.currentTimeMillis());
//            }
//        }catch (InterruptedException e){
//            //e.printStackTrace();
//            return null;
//        }
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
                throw new Exception("rejected");
            }
            return doc;
        }catch (Exception e){
            //e.printStackTrace();
            System.out.println("fail proxy ip " + proxy.ip);
            proxyExtractor.failProxy(proxy);
            throw e;
        }
    }

    public Document crawlOneProduct(String productId) throws Exception{
        return crawlOneProduct(productId, true);
    }


    public static void main(String[] args) {
        MovieInfoCrawler crawler = new MovieInfoCrawler();
        try{
            Document doc = crawler.crawlOneProduct("0001489305");
            System.out.println(doc);
        }catch (Exception e){
            e.printStackTrace();
        }
    }


}
