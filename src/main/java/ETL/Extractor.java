package ETL;
import java.io.*;
import java.net.URL;
import java.util.HashMap;

public class Extractor {
    private static BufferedReader reviewData;

    public static boolean init(){
        try{
            File file = new File("/Users/purchaser/Desktop/movies.txt");
            reviewData = new BufferedReader(new FileReader(file));
            return true;
        }catch (FileNotFoundException e){
            e.printStackTrace();
            return false;
        }
    }

    public static HashMap<String, String> getNextRawReview(){
        assert reviewData != null;
        HashMap<String, String> review = new HashMap<String, String>();
        while(true){
            String line;
            try{
                line = reviewData.readLine();
                if(line == null){
                    return null;
                }
                if(line.equals("")){
                    return review;
                }
                String[] keyAndValue = line.split(": ", 2);
                if(keyAndValue.length != 2){
                    System.out.println(keyAndValue[0]);
                    System.out.println(review);
                    while(!line.equals("") && line != null){
                        line = reviewData.readLine();
                    }
                    review.clear();
                    return review;
                }
                review.put(keyAndValue[0], keyAndValue[1]);
            }catch (IOException e){
                e.printStackTrace();
                return null;
            }
        }

    }
}
