package MovieInfoCrawl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

public class FileProductIdsExtractor implements ProductIdsExtractor{
    public static ArrayList<String> productIds = new ArrayList<>();

    private static String productIdsCsvPath;
    static {
        try{
            Properties props = new Properties();
            props.load(Main.class.getClassLoader().getResourceAsStream("baseDir.properties"));
            productIdsCsvPath = props.getProperty("productIdsCsv");
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    static{
        try{
            File productIdsCsvFile = new File(productIdsCsvPath);
            if(!productIdsCsvFile.exists()){
                throw new Exception("productIdsCsv 文件不存在！");
            }
            BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(productIdsCsvPath)));
            bufferedReader.readLine();
            while(true){
                String productId = bufferedReader.readLine();
                if(productId == null){
                    break;
                }
                productIds.add(productId);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public ArrayList<String> getProductIdsForRange(int startFrom, int limitation){
        return new ArrayList<>(productIds.subList(startFrom, limitation));
    }

    public String getProductId(int index){
        return productIds.get(index);
    }

    public int getProductIdsCount(){
        return productIds.size();
    }

    public static void main(String[] args) {
        FileProductIdsExtractor dbProductIdsExtractor = new FileProductIdsExtractor();
        ArrayList<String> ps = dbProductIdsExtractor.getProductIdsForRange(0, 100);
        System.out.println(ps);
        System.out.println(ps.size());
    }

}
