package MovieInfoCrawl;

import Entities.ProductBundle;
import org.jsoup.nodes.Document;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class Main {

    private static String baseDir;
    private static Integer retryTime;
    private static BufferedWriter failedProductIdsCsvFileWriter;
    static {
        try{
            Properties props = new Properties();
            props.load(Main.class.getClassLoader().getResourceAsStream("baseDir.properties"));
            baseDir = props.getProperty("baseDir");
            retryTime = Integer.parseInt(props.getProperty("retryTime"));
            File failedProductIdsCsvFile = new File(props.getProperty("failedProductIdsCsv"));
            if(!failedProductIdsCsvFile.exists()){
                failedProductIdsCsvFile.createNewFile();
            }
            failedProductIdsCsvFileWriter = new BufferedWriter(new FileWriter(failedProductIdsCsvFile));
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    private final static int BATCH_SIZE = 4000;

    private final static int bundleBufferCapcity = 20;

    private final static int CRAWLER_THREADS_NUM = 5;

    private final static int START_BATCH_INDEX = 0;

    private final static int THREAD_POOL_SIZE = 10;

    public static void main(String[] args) {
        doCrawlAndSave();
    }

    static void doCrawlAndSave() {
        File htmlsDir = new File(baseDir + "/htmls");
        int startIndex = htmlsDir.listFiles().length;
        ProductIdsExtractor productIdsExtractor = new FileProductIdsExtractor();

        MovieInfoCrawler crawler = new MovieInfoCrawler();

        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        for(int currIndex = startIndex; currIndex < productIdsExtractor.getProductIdsCount(); currIndex++){
            executorService.submit(new StreamProcessRunner(crawler, productIdsExtractor.getProductId(currIndex), currIndex , retryTime));
        }
        executorService.shutdown();
        while(!executorService.isTerminated()){
            try{
                Thread.sleep(60000);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }

    }

    static void doCrawlAndParse(){
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(sdf.format(date));
        String logFileUrl = baseDir + "logFile_" + sdf.format(date) + ".txt";
        File logFile = new File(logFileUrl);
        if(!logFile.exists()){
            try{
                if(!logFile.createNewFile()){
                    System.out.println("create log file failed");
                    return;
                }
            }catch (IOException e){
                e.printStackTrace();
                return;
            }
        }
        try {
            PrintStream pm = new PrintStream(logFileUrl);
            System.setOut(pm);
        }catch (FileNotFoundException e){
            System.out.println("log file not found");
        }
        System.out.println(sdf.format(date));
        ProductIdsExtractor productIdsExtractor = new FileProductIdsExtractor();

        int productIdsCount = productIdsExtractor.getProductIdsCount();
        int loopTime = productIdsCount / BATCH_SIZE + 1;

        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        for(int i = START_BATCH_INDEX; i < loopTime; i++){
            executorService.submit(new BatchProcessThread(i));
        }
        executorService.shutdown();
        while(!executorService.isTerminated()){
            try{
                Thread.sleep(60000);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }

    static class StreamProcessRunner implements Runnable{
        private String productId;
        private MovieInfoCrawler crawler;
        private int retryTime;
        private int currIndex;
        StreamProcessRunner(MovieInfoCrawler crawler,
                            String productId,
                            int currIndex,
                            int retryTime){
            this.productId = productId;
            this.crawler = crawler;
            this.retryTime = retryTime;
            this.currIndex = currIndex;
        }
        @Override
        public void run() {
            try{

                if(productId == null){
                    return;
                }
                Document doc;
                boolean success = false;
                while(retryTime > 0){
                    doc = crawler.crawlOneProduct(productId);
                    if(doc == null){
                        System.out.println("Crawler" + Thread.currentThread().getId() + " crawling " + productId + " failed... cause: cannot access  retryTime left:" + retryTime);
                        //retry
                        retryTime--;
                        Thread.sleep(5000);
                        continue;
                    }
                    success = MovieInfoTransformer.parseAndSaveDoc(productId, doc);
                    if(!success){
                        System.out.println("Crawler" + Thread.currentThread().getId() + " crawling " + productId + " failed... cause: rejected retryTime left:" + retryTime);
                        retryTime--;
                        Thread.sleep(5000);
                        continue;
                    }
                    break;
                }
                if(!success){
                    System.out.println("Crawler" + Thread.currentThread().getId() + " crawling " + productId + " failed");
                    //TODO 写入文件，记录失败
                    failedProductIdsCsvFileWriter.write(productId + "\n");
                    return;
                }
                System.out.println("Crawler" + Thread.currentThread().getId() + " got product " + productId);

            }catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }
    }

    static class BatchProcessThread extends Thread{
        private LinkedBlockingQueue<String> productIdsBuffer = new LinkedBlockingQueue<>(BATCH_SIZE);
        private LinkedBlockingQueue<ProductBundle> bundleBuffer = new LinkedBlockingQueue<>(bundleBufferCapcity);
        private boolean crawlersOverFlag = false;
        private int batchIndex;
        private int batchSize = BATCH_SIZE;
        private ProductIdsExtractor productIdsExtractor = new FileProductIdsExtractor();
        BatchProcessThread(int batchIndex){
            this.batchIndex = batchIndex;
        }
        @Override
        public void run(){
            doBatch(batchIndex, batchSize);
        }

        private boolean doBatch(int batchIndex, int batchSize){
            //return true if there are more products left
            String batchRange = (batchIndex * batchSize) + "-" + ((batchIndex+1) * batchSize);
            crawlersOverFlag = false;

            MovieInfoCrawler crawler = new MovieInfoCrawler();
            MovieInfoLoader loader = new MovieInfoLoader();

            System.out.println("starting batch " + batchRange);

            bundleBuffer = new LinkedBlockingQueue<>(bundleBufferCapcity);
            productIdsBuffer = new LinkedBlockingQueue<>(BATCH_SIZE);

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
            loader.switchFileHandles(baseDir + dirName + "/");

            //step3: write CSV headers
            loader.loadHeaders();

            //step4: start loading productIds
            boolean noMoreProducts = false;
            ArrayList<String> productIds = productIdsExtractor.getProductIdsForRange(batchIndex * BATCH_SIZE, BATCH_SIZE);

            System.out.println("Batch - " + batchRange + ": " +  Thread.currentThread().getId() + " got productIds: " + productIds.size());

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
            LoaderRunner loaderRunner = new LoaderRunner(loader);

            for (int i = 0; i < CRAWLER_THREADS_NUM; i++) {
                crawlers.add(new CrawlerRunner(crawler));
                crawlers.get(i).start();
            }
            loaderRunner.start();



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
                loaderRunner.join();

            }catch (InterruptedException e){
                e.printStackTrace();
            }

            System.out.println("finishing batch " + batchRange);

            return !noMoreProducts;

        }


        class CrawlerRunner extends Thread{
            private MovieInfoCrawler crawler;
            CrawlerRunner(MovieInfoCrawler crawler){
                this.crawler = crawler;
            }

            @Override
            public void run(){
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
                            bundle = MovieInfoTransformer.parseDoc(productId, doc);
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

        class LoaderRunner extends Thread{
            private MovieInfoLoader realloader;
            LoaderRunner(MovieInfoLoader loader){
                realloader = loader;
            }
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
                    realloader.loadBundles(bundles);
                }
                System.out.println("Loader" + Thread.currentThread().getId() + " exiting...");
            }
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




}
