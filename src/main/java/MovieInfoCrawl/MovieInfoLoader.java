package MovieInfoCrawl;

import Entities.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.Buffer;
import java.util.ArrayList;
import java.util.Collections;

public class MovieInfoLoader {

    private BufferedWriter productDetailFileHandle;
    private BufferedWriter productFormatFileHandle;
    private BufferedWriter productActorFileHandle;


    public void switchFileHandles(String baseDir){
        try{
            productActorFileHandle = new BufferedWriter(new FileWriter(baseDir + "productActorFile.csv"));
            productDetailFileHandle = new BufferedWriter(new FileWriter(baseDir + "productDetailFile.csv"));
            productFormatFileHandle = new BufferedWriter(new FileWriter(baseDir + "productFormatFile.csv"));
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public void loadBundles(ArrayList<ProductBundle> productBundles){
        StringBuilder builderDetail = new StringBuilder();
        StringBuilder builderActors = new StringBuilder();
        StringBuilder builderFormats = new StringBuilder();

        for (ProductBundle productBundle: productBundles) {
            ProductDetail detail = productBundle.getProductDetail();
            ArrayList<ProductFormat> formats = productBundle.getProductFormats();
            ArrayList<ProductActor> actors = productBundle.getProductActors();

            buildOneCSVLine(builderDetail, detail);

            for(ProductActor actor: actors){
                buildOneCSVLine(builderActors, actor);
            }

            for(ProductFormat format: formats){
                buildOneCSVLine(builderFormats, format);
            }
        }
        String detailStr = builderDetail.toString();
        String actorsStr = builderActors.toString();
        String formatsStr = builderFormats.toString();
        //System.out.println("About to write into file...");
        //System.out.println("=== " + detailStr + actorsStr + formatsStr + "===============");

        writeCSV(detailStr, productDetailFileHandle);
        writeCSV(actorsStr, productActorFileHandle);
        writeCSV(formatsStr, productFormatFileHandle);

    }

    public void loadHeaders(){
        String detailHeader = buildCSVHeaderLine(ProductDetail.class);
        String formatHeader = buildCSVHeaderLine(ProductFormat.class);
        String actorHeader = buildCSVHeaderLine(ProductActor.class);
        writeCSV(detailHeader, productDetailFileHandle);
        writeCSV(actorHeader, productActorFileHandle);
        writeCSV(formatHeader, productFormatFileHandle);
    }

    private static void buildOneCSVLine(StringBuilder builder, Object object){
        try{
            Method[] actorMethods = object.getClass().getDeclaredMethods();
            for(Method method: actorMethods){
                if(method.getName().startsWith("get")){
                    builder.append(method.invoke(object).toString());
                    builder.append(',');
                }
            }
            builder.deleteCharAt(builder.length() - 1);
            builder.append('\n');
        }catch (Exception e){
            //shouldn't happen
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }

    private static String buildCSVHeaderLine(Class cls){
        StringBuilder builder = new StringBuilder();
        try{
            Method[] actorMethods = cls.getDeclaredMethods();
            for(Method method: actorMethods){
                if(method.getName().startsWith("get")){
                    builder.append(method.getName().split("get")[1]);
                    builder.append(',');
                }
            }
            builder.deleteCharAt(builder.length() - 1);
            builder.append('\n');
        }catch (Exception e){
            //shouldn't happen
            e.printStackTrace();
            System.out.println(e.getMessage());
            return "";
        }
        return builder.toString();
    }

    private static void writeCSV(String content, BufferedWriter writer){
        try{
            synchronized (writer){
                writer.write(content);
                writer.flush();
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }



    public static void main(String[] args) {
        StringBuilder builder = new StringBuilder();
        ProductFormat productFormat = new ProductFormat("123", "qwe");
        buildOneCSVLine(builder, productFormat);
        System.out.println(builder.toString());
    }
}
