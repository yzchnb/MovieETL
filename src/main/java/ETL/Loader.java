package ETL;

import Entities.Product;
import Entities.Review;
import Entities.User;

import java.sql.*;

public class Loader {
    private Connection conn;

    public void init(Connection conn){
        this.conn = conn;
    }

    public boolean insertOneReview(User user, Review review, Product product){
        try{
            PreparedStatement stmt = null;
            try{
                String insertUserSql = "insert into User(userId) values(?)";
                stmt = conn.prepareStatement(insertUserSql);
                stmt.setString(1, user.getUserId());
                stmt.executeUpdate();

                String insertProductSql = "insert into Product(productId) values(?)";
                stmt = conn.prepareStatement(insertProductSql);
                stmt.setString(1, product.getProductId());
                stmt.executeUpdate();
            }catch (SQLException e){
                //
            }

            String insertReviewSql = "insert into Review(userId, productId, profileName, voters, supporters, score, time, summary, text)" +
                    "values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
            stmt = conn.prepareStatement(insertReviewSql);
            stmt.setString(1, user.getUserId());
            stmt.setString(2, product.getProductId());
            stmt.setString(3, review.getProfileName());
            stmt.setInt(4, review.getVoters());
            stmt.setInt(5, review.getSupporters());
            stmt.setFloat(6, review.getScore());
            stmt.setTimestamp(7, new Timestamp(Long.parseLong(review.getTime())));
            stmt.setString(8, review.getSummary());
            stmt.setString(9, review.getText());
            stmt.executeUpdate();

        }catch (SQLException e){
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private boolean userExists(User user){
        try{
            PreparedStatement stmt = null;
            String querySql = "select count(userId) from User where userId = (?)";
            stmt = conn.prepareStatement(querySql);
            stmt.setString(1, user.getUserId());
            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1) == 1;
        }catch (SQLException e){
            e.printStackTrace();
            return false;
        }
    }

    private boolean productExists(Product product){
        try{
            PreparedStatement stmt = null;
            String querySql = "select count(productId) from Product where productId = (?)";
            stmt = conn.prepareStatement(querySql);
            stmt.setString(1, product.getProductId());
            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1) == 1;
        }catch (SQLException e){
            e.printStackTrace();
            return false;
        }
    }
}
