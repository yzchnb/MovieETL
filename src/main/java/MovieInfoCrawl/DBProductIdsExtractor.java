package MovieInfoCrawl;

import Utils.C3P0Mysql;

import java.sql.*;
import java.util.ArrayList;

public class DBProductIdsExtractor implements ProductIdsExtractor{
    private static Connection conn;

    static {
        C3P0Mysql conns = C3P0Mysql.getInstance();
        conn = conns.getConnection();
    }

    public ArrayList<String> productIds;

    public ArrayList<String> getProductIdsForRange(int startFrom, int limitation){
        try{
            String sql = "select productId from Product limit ?, ?";

            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setInt(1, startFrom);
            stmt.setInt(2, limitation);
            ResultSet rs = stmt.executeQuery();
            //System.out.println(rs.getFetchSize());
            ArrayList<String> productIds = new ArrayList<>(limitation);
            while(rs.next()){
                productIds.add(rs.getString(1));
            }
            return productIds;
        }catch (SQLException e){
            e.printStackTrace();
            return null;
        }
    }

    public int getProductIdsCount(){
        try{
            String sql = "select count(distinct productId) from Product";

            PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            //System.out.println(rs.getFetchSize());
            while(rs.next()){
                return rs.getInt(1);
            }
            return 0;
        }catch (SQLException e){
            e.printStackTrace();
            return 0;
        }

    }

    @Override
    public String getProductId(int index) {
        return null;
    }

    public static void main(String[] args) {
        DBProductIdsExtractor dbProductIdsExtractor = new DBProductIdsExtractor();
        ArrayList<String> ps = dbProductIdsExtractor.getProductIdsForRange(0, 100);
        System.out.println(ps);
        System.out.println(ps.size());
    }

}
