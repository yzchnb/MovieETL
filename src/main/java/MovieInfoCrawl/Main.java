package MovieInfoCrawl;

import ETL.Loader;
import Entities.ProductBundle;
import org.eclipse.jetty.util.IO;
import org.jsoup.nodes.Document;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class Main {
    private static final String baseDir = "/Users/purchaser/Desktop/MovieInfos/";

    private final static int BATCH_SIZE = 1000;

    private static LinkedBlockingQueue<String> productIdsBuffer = new LinkedBlockingQueue<>(BATCH_SIZE);

    //private final static int docBufferCapcity = 5;
    //private static LinkedBlockingQueue<Pair<Document, String>> docBuffer = new LinkedBlockingQueue<>(docBufferCapcity);

    private final static int bundleBufferCapcity = 20;
    private static LinkedBlockingQueue<ProductBundle> bundleBuffer = new LinkedBlockingQueue<>(bundleBufferCapcity);

    //private final static int BUNDLE_LOAD_BUFFER_NUM = 1;

    private final static int CRAWLER_THREADS_NUM = 10;

    //private final static int TRANSFORMER_THREADS_NUM = 1;

    private final static int LOADER_THREADS_NUM = 1;

    private static boolean crawlersOverFlag = false;

    //private static boolean transformersOverFlag = false;

    public static void main(String[] args) {
        String logFileUrl = baseDir + "logFile.txt";
        File logFile = new File(logFileUrl);
        if(!logFile.exists()){
            try{
                if(!logFile.createNewFile()){
                    System.out.println("create log file failed");
                    return;
                }
                PrintStream pm = new PrintStream(logFileUrl);
                System.setOut(pm);
            }catch (IOException e){
                e.printStackTrace();
                return;
            }
        }
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(sdf.format(date));


        int batchIndex = 0;
        while(doBatch(batchIndex, BATCH_SIZE)){
            batchIndex++;
        }

    }

    private static boolean doBatch(int batchIndex, int batchSize){
        //return true if there are more products left
        String batchRange = (batchIndex * batchSize) + "-" + ((batchIndex+1) * batchSize);
        crawlersOverFlag = false;
        //transformersOverFlag = false;

        System.out.println("starting batch " + batchRange);

        bundleBuffer = new LinkedBlockingQueue<>(bundleBufferCapcity);
        productIdsBuffer = new LinkedBlockingQueue<>(BATCH_SIZE);
        //docBuffer = new LinkedBlockingQueue<>(docBufferCapcity);

        //step1: new directories
        String dirName = "Batch_" + batchRange;
        File dir = new File(baseDir + dirName);
        if(!dir.exists()){
            if(!dir.mkdir()){
                System.out.println("mkdir failed:" + dir.getAbsolutePath());
                return true;
            }
        }
        //step2: change static file handles
        MovieInfoLoader.switchFileHandles(baseDir + dirName + "/");

        //step3: write CSV headers
        MovieInfoLoader.loadHeaders();

        //step4: start loading productIds
        boolean noMoreProducts = false;
        ArrayList<String> productIds = ProductIdsExtractor.getProductIdsForRange(batchIndex * BATCH_SIZE, BATCH_SIZE);

        if(productIds.size() == 0){
            noMoreProducts = true;
        }
        for (int i = 0; i < productIds.size(); i++) {
            try{
                productIdsBuffer.put(productIds.get(i));
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }

        //step5: init threads
        ArrayList<CrawlerRunner> crawlers = new ArrayList<>(CRAWLER_THREADS_NUM);
        ArrayList<LoaderRunner> loaders = new ArrayList<>(LOADER_THREADS_NUM);

        for (int i = 0; i < CRAWLER_THREADS_NUM; i++) {
            crawlers.add(new CrawlerRunner());
            crawlers.get(i).start();
        }
        for (int i = 0; i < LOADER_THREADS_NUM; i++) {
            loaders.add(new LoaderRunner());
            loaders.get(i).start();
        }



        //step6: wait all threads finish
        //we wait until all the loader thread take the bundles
        while(productIdsBuffer.size() != 0);

        //We cannot join those threads because inside they are infinite loops
        //We just interrupt them, so they could escape the infinite loop
        try{
            for (int i = 0; i < CRAWLER_THREADS_NUM; i++) {
                crawlers.get(i).join();
            }
            crawlersOverFlag = true;
            for (int i = 0; i < LOADER_THREADS_NUM; i++) {
                loaders.get(i).join();
            }

        }catch (InterruptedException e){
            e.printStackTrace();
        }

        System.out.println("finishing batch " + batchRange);

        return !noMoreProducts;

    }


    static class CrawlerRunner extends Thread{

        @Override
        public void run(){
            MovieInfoCrawler crawler = new MovieInfoCrawler();
            while(productIdsBuffer.size() > 0){
                try{
                    String productId = productIdsBuffer.poll();
                    if(productId == null){
                        continue;
                    }
                    int retryTime = 3;
                    Document doc;
                    ProductBundle bundle = null;
                    while(retryTime > 0){
                        doc = crawler.crawlOneProduct(productId);
                        if(doc == null){
                            System.out.println("Crawler" + Thread.currentThread().getId() + " crawling " + productId + " failed... cause: cannot access  retryTime left:" + retryTime);
                            //retry
                            retryTime--;
                            Thread.sleep(5000);
                            continue;
                        }
                        bundle = MovieInfoTransformer.ParseDoc(productId, doc);
                        if(bundle == null){
                            System.out.println("Crawler" + Thread.currentThread().getId() + " crawling " + productId + " failed... cause: rejected retryTime left:" + retryTime);
                            retryTime--;
                            Thread.sleep(5000);
                            continue;
                        }
                        break;
                    }
                    if(bundle == null){
                        System.out.println("Crawler" + Thread.currentThread().getId() + " crawling " + productId + " failed");
                        continue;
                    }

                    System.out.println("Crawler" + Thread.currentThread().getId() + " got product " + productId);
                    Thread.sleep(1000);
                    bundleBuffer.put(bundle);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
            System.out.println("Crawler" + Thread.currentThread().getId() + " exiting...");
        }
    }

//    static class TransformerRunner extends Thread{
//        @Override
//        public void run(){
//            while (!crawlersOverFlag){
//                try{
//                    Pair<Document, String> docAndId = docBuffer.poll();
//                    if(docAndId == null){
//                        continue;
//                    }
//                    System.out.println("Transformer: " + Thread.currentThread().getId() + ": got doc: " + docAndId.second);
//                    ProductBundle bundle = MovieInfoTransformer.ParseDoc(docAndId.second, docAndId.first);
//                    if(bundle == null){
//                        System.out.println("Transformer: " + Thread.currentThread().getId() + " parsing doc " + docAndId.second +" failed " );
//                        continue;
//                    }
//                    bundleBuffer.put(bundle);
//                }catch (InterruptedException e){
//                    e.printStackTrace();
//                    return;
//                }
//            }
//            System.out.println("Transformer: " + Thread.currentThread().getId() + " exiting...");
//        }
//    }

    static class LoaderRunner extends Thread{
        @Override
        public void run(){
//            while(true){
////                ArrayList<ProductBundle> bundles = new ArrayList<>(BUNDLE_LOAD_BUFFER_NUM);
////                try{
////                    while(bundles.size() < BUNDLE_LOAD_BUFFER_NUM && !Thread.currentThread().isInterrupted()){
////                        ProductBundle bundle = bundleBuffer.poll();
////                        if(bundle == null){
////                            continue;
////                        }
////                        bundles.add(bundle);
////                        System.out.println("Loader: " + Thread.currentThread().getId() + " got: " + bundles.get(bundles.size() - 1).getProductDetail().getProductId());
////                    }
////                    if(Thread.currentThread().isInterrupted()){
////                        break;
////                    }
////                } finally {
////                    System.out.println("Loader: "+ Thread.currentThread().getId() + " preparing load into file");
////                    MovieInfoLoader.loadBundles(bundles);
////                }
////            }
            while(!crawlersOverFlag){
                ProductBundle bundle = bundleBuffer.poll();
                if(bundle == null){
                    continue;
                }
                ArrayList<ProductBundle> bundles = new ArrayList<>();
                bundles.add(bundle);
                System.out.println("Loader" + Thread.currentThread().getId() + " got bundle " + bundle.getProductDetail().getProductId());
                MovieInfoLoader.loadBundles(bundles);
            }
            System.out.println("Loader" + Thread.currentThread().getId() + " exiting...");
        }
    }



}
