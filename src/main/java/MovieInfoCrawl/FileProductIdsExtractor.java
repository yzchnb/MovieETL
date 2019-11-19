package MovieInfoCrawl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;

public class FileProductIdsExtractor {
    public static ArrayList<String> productIds = new ArrayList<>();

    private static String productIdsCsvPath;
    private static String baseDir;
    static {
        try{
            Properties props = new Properties();
            props.load(Main.class.getClassLoader().getResourceAsStream("baseDir.properties"));
            productIdsCsvPath = props.getProperty("productIdsCsv");
            baseDir = props.getProperty("baseDir");
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
            File htmls = new File(baseDir + "/htmls");
            if(!htmls.exists()){
                htmls.mkdir();
            }
            HashSet<String> alreadyGeneratedSet = new HashSet<>();
            for (String s : htmls.list()) {
                alreadyGeneratedSet.add(s.split("\\.")[0]);
            }
            productIds.removeAll(alreadyGeneratedSet);

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

    public int getIndexOfProductId(String productId){
        return productId == null ? 0 : productIds.indexOf(productId);
    }

    public static void main(String[] args) {
        FileProductIdsExtractor dbProductIdsExtractor = new FileProductIdsExtractor();
        ArrayList<String> ps = dbProductIdsExtractor.getProductIdsForRange(0, 100);
        System.out.println(ps);
        System.out.println(ps.size());
    }

}
