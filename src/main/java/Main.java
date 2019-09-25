import ETL.Extractor;
import ETL.Loader;
import ETL.Transfomer;
import Entities.Product;
import Entities.Review;
import Entities.User;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    public static void main(String[] args) throws SQLException {

        int poolNum = 50;
        System.out.println("Preparing connections");
        C3P0Mysql conns = C3P0Mysql.getInstance();
        System.out.println("Connections established");
        prepareFile();


        ExecutorService pool = Executors.newFixedThreadPool(50);


        int count = 0;
        out: while(true){
            for (int i = 0; i < 1000; i++) {
                HashMap<String, String> rawReview = Extractor.getNextRawReview();
                if(rawReview == null){
                    break out;
                }
                if(!Transfomer.checkRawReviewValidity(rawReview)){
                    continue;
                }
                User user = Transfomer.getUser(rawReview);
                Review review = Transfomer.getReview(rawReview);
                Product product = Transfomer.getProduct(rawReview);
                LoadTask loadTask = new LoadTask(conns.getConnection(), user, product, review);
                pool.submit(loadTask);
                count += 1;
                //System.out.println(count);
            }
            System.out.println(count);
        }

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
