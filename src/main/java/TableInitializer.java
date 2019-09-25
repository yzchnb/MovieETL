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

public class TableInitializer {

    public static void main(String[] args) throws SQLException {

        if(!Extractor.init()){
            System.out.println("open failed");
        }
        int count = 0;

        int profileNameLong = 0, summaryLong = 0, textLong = 0;
        out: while(true){
            for (int i = 0; i < 1000; i++) {
                HashMap<String, String> rawReview = Extractor.getNextRawReview();
                if(rawReview == null){
                    break out;
                }
                if(!Transfomer.checkRawReviewValidity(rawReview)){
                    continue;
                }
                Review review = Transfomer.getReview(rawReview);
                profileNameLong = Math.max(review.getProfileName().length(), profileNameLong);
                summaryLong = Math.max(review.getSummary().length(), summaryLong);
                textLong = Math.max(review.getText().length(), textLong);
                count += 1;
            }
            System.out.println(count);
        }
        System.out.println("profileName: " + profileNameLong);
        System.out.println("summaryLong: " + summaryLong);
        System.out.println("textLong: " + textLong);
    }
}
