package ETL;

import Entities.Product;
import Entities.Review;
import Entities.User;

import java.util.HashMap;

public class Transfomer {

    private static String userIdKey = "review/userId";
    private static String productIdKey = "product/productId";
    private static String profileNameKey = "review/profileName";
    private static String helpfulnessKey = "review/helpfulness";
    private static String scoreKey = "review/score";
    private static String timeKey = "review/time";
    private static String summaryKey = "review/summary";
    private static String textKey = "review/text";

    public static User getUser(HashMap<String, String> rawReviewData){
        User user = new User();
        user.setUserId(rawReviewData.get(userIdKey));
        return user;
    }

    public static Review getReview(HashMap<String, String> rawReviewData){
        Review review = new Review();
        String helpfulness = rawReviewData.get(helpfulnessKey);
        String[] supportersAndVoters = helpfulness.split("/", 2);
        review.setVoters(Integer.parseInt(supportersAndVoters[1]));
        review.setSupporters(Integer.parseInt(supportersAndVoters[0]));
        review.setProfileName(rawReviewData.get(profileNameKey));
        review.setScore(Float.parseFloat(rawReviewData.get(scoreKey)));
        review.setSummary(rawReviewData.get(summaryKey));
        review.setText(rawReviewData.get(textKey));
        review.setTime(rawReviewData.get(timeKey));
        return review;
    }

    public static Product getProduct(HashMap<String, String> rawReviewData){
        Product product = new Product();
        product.setProductId(rawReviewData.get(productIdKey));
        return product;
    }

    public static boolean checkRawReviewValidity(HashMap<String, String> rawReviewData){
        return rawReviewData.containsKey(userIdKey)
                && rawReviewData.containsKey(helpfulnessKey)
                && rawReviewData.containsKey(productIdKey)
                && rawReviewData.containsKey(scoreKey)
                && rawReviewData.containsKey(timeKey)
                && rawReviewData.containsKey(profileNameKey)
                && rawReviewData.containsKey(summaryKey)
                && rawReviewData.containsKey(textKey)
                && rawReviewData.get(userIdKey).length() <= 14
                && rawReviewData.get(helpfulnessKey).length() <= 10
                && rawReviewData.get(productIdKey).length() <= 10
                && rawReviewData.get(profileNameKey).length() <= 50
                && rawReviewData.get(summaryKey).length() <= 255
                && rawReviewData.get(textKey).length() <= 35000;
    }
}
