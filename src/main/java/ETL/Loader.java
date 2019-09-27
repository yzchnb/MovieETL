package ETL;

import Entities.Product;
import Entities.Review;
import Entities.User;

import java.sql.*;
import java.util.ArrayList;

public class Loader {
    private Connection conn;

    public void init(Connection conn){
        this.conn = conn;
    }

    public boolean insertReviews(ArrayList<User> users, ArrayList<Review> reviews, ArrayList<Product> products){
        assert users.size() == reviews.size() && reviews.size() == products.size();
        int size = users.size();
        try{
            PreparedStatement stmt;
            //System.out.println("inserting users...");
            // No.1 sql insertUser
            String insertUserSql = "replace into User(userId) values(?)";
            stmt = conn.prepareStatement(insertUserSql);
            for (int i = 0; i < size; i++) {
                stmt.setString(1, users.get(i).getUserId());
                stmt.addBatch();
            }
            synchronized(Loader.class){
                stmt.executeBatch();
                conn.commit();
                stmt.close();
            }
            //System.out.println("insert users over.");


            //System.out.println("inserting products...");
            //No.2 sql insertProduct
            String insertProductSql = "replace into Product(productId) values(?)";
            stmt = conn.prepareStatement(insertProductSql);
            for (int i = 0; i < size; i++) {
                stmt.setString(1, products.get(i).getProductId());
                stmt.addBatch();
            }
            synchronized (Loader.class){
                stmt.executeBatch();
                conn.commit();
                stmt.close();
            }
            //System.out.println("insert products over.");


            //System.out.println("inserting reviews...");
            //No.3 sql insertReview
            String insertReviewSql = "insert into Review(userId, productId, profileName, voters, supporters, score, time, summary, text)" +
                    "values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
            stmt = conn.prepareStatement(insertReviewSql);
            for (int i = 0; i < size; i++) {
                Review review = reviews.get(i);
                stmt.setString(1, users.get(i).getUserId());
                stmt.setString(2, products.get(i).getProductId());
                stmt.setString(3, review.getProfileName());
                stmt.setInt(4, review.getVoters());
                stmt.setInt(5, review.getSupporters());
                stmt.setFloat(6, review.getScore());
                stmt.setTimestamp(7, new Timestamp(Long.parseLong(review.getTime())));
                stmt.setString(8, review.getSummary());
                stmt.setString(9, review.getText());
                stmt.addBatch();
            }
            stmt.executeBatch();
            conn.commit();
            //System.out.println("insert reviews over.");

            stmt.close();
            conn.close();
        }catch (SQLException e){
            e.printStackTrace();
            return false;
        }

        return true;
    }


}
