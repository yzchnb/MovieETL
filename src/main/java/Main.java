import ETL.Extractor;
import ETL.Transfomer;
import Entities.Product;
import Entities.Review;
import Entities.User;
import Utils.C3P0Mysql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    public static void main(String[] args) throws SQLException {

        long startTime = System.currentTimeMillis();

        int bufferCapacity = 1000;
        int poolNum = 30;
        System.out.println("Preparing connections");
        C3P0Mysql conns = C3P0Mysql.getInstance();
        System.out.println("Connections established");
        prepareFile();


        ExecutorService pool = Executors.newFixedThreadPool(poolNum);

        int count = 0;
        out: while(true){

            ArrayList<User> userBuffer = new ArrayList<User>(bufferCapacity);
            ArrayList<Product> productBuffer = new ArrayList<Product>(bufferCapacity);
            ArrayList<Review> reviewBuffer = new ArrayList<Review>(bufferCapacity);

            for (int i = 0; i < bufferCapacity; i++) {
                HashMap<String, String> rawReview = Extractor.getNextRawReview();
                if(rawReview == null){
                    break out;
                }
                if(!Transfomer.checkRawReviewValidity(rawReview)){
                    continue;
                }
                userBuffer.add(Transfomer.getUser(rawReview));
                reviewBuffer.add(Transfomer.getReview(rawReview));
                productBuffer.add(Transfomer.getProduct(rawReview));
                count += 1;
            }
            //System.out.println(userBuffer.size());
            LoadTask loadTask = new LoadTask(conns.getConnection(), userBuffer, productBuffer, reviewBuffer);
            pool.submit(loadTask);
            //Loader loader = new Loader();
            //loader.init(conns.getConnection());
            //loader.insertReviews(userBuffer, reviewBuffer, productBuffer);
            System.out.println(count);
        }

        pool.shutdown();
        try{
            while(!pool.isTerminated()){
                Thread.sleep(1000);
            }
        }catch (InterruptedException e){
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();

        System.out.println("Running time: " + (endTime - startTime) / 1000 + "s");
    }

    static void prepareFile(){

        System.out.println("Starting ETL process");

        System.out.println("Opening Movie.txt");
        if(!Extractor.init()){
            System.out.println("open failed");
            return;
        }
        System.out.println("File opened successfully");
    }

}
