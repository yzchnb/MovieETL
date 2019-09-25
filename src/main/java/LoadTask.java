import ETL.Loader;
import Entities.Product;
import Entities.Review;
import Entities.User;

import java.sql.Connection;
import java.sql.SQLException;

public class LoadTask implements Runnable{
    private User user;
    private Review review;
    private Product product;
    private Connection conn;

    public LoadTask(Connection conn, User user, Product product, Review review){
        this.conn = conn;
        this.review = review;
        this.user = user;
        this.product = product;
    }

    public void run(){
        Loader loader = new Loader();
        loader.init(conn);
        loader.insertOneReview(user, review, product);
        try{
            conn.close();
        }catch (SQLException e){
            e.printStackTrace();
            System.out.println("conn close fail");
        }
    }

}
