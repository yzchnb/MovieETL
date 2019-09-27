import ETL.Loader;
import Entities.Product;
import Entities.Review;
import Entities.User;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

public class LoadTask implements Runnable{
    private ArrayList<User> users;
    private ArrayList<Review> reviews;
    private ArrayList<Product> products;
    private Connection conn;

    public LoadTask(Connection conn, ArrayList<User> userBuffer, ArrayList<Product> productBuffer, ArrayList<Review> reviewBuffer){
        this.conn = conn;
        this.reviews = reviewBuffer;
        this.users = userBuffer;
        this.products = productBuffer;
    }

    public void run(){
        Loader loader = new Loader();
        loader.init(conn);
        loader.insertReviews(users, reviews, products);
    }

}
