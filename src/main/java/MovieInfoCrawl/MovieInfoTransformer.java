package MovieInfoCrawl;

import Entities.ProductActor;
import Entities.ProductBundle;
import Entities.ProductDetail;
import Entities.ProductFormat;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

public class MovieInfoTransformer {

    private static String baseDir;
    static {
        try{
            Properties props = new Properties();
            props.load(Main.class.getClassLoader().getResourceAsStream("baseDir.properties"));
            baseDir = props.getProperty("baseDir");
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public static ProductBundle parseDoc(String productId, Document doc){
        Elements lis = doc.select("#detail-bullets > table > tbody > tr > td > div > ul > li");
        if(lis.size() == 0){
            return null;
        }
        String[] actors = {}, formats = {};
        String director = "", releaseDate = "";
        String runTime = "";

        for (int i = 0; i < lis.size(); i++) {
            Element e = lis.get(i);
            switch (e.children().first().html()){
                case "Actors:":
                    actors = processActors(e);
                    break;
                case "Directors:":
                    director = processDirector(e);
                    break;
                case "Format:":
                    formats = processFormat(e);
                    break;
                case "DVD Release Date:":
                    releaseDate = processReleaseDate(e);
                    break;
                case "Run Time:":
                    runTime = processRunTime(e);
                    break;
                default:
            }
        }

        ArrayList<ProductActor> productActors = new ArrayList<>(actors.length);
        ArrayList<ProductFormat> productFormats = new ArrayList<>(formats.length);
        ProductDetail productDetail = new ProductDetail(productId, releaseDate, director, runTime);
        for (String actor: actors) {
            productActors.add(new ProductActor(productId, actor));
        }
        for (String format: formats){
            productFormats.add(new ProductFormat(productId, format));
        }
        return new ProductBundle(productDetail, productActors, productFormats);
    }

    public static boolean parseAndSaveDoc(String productId, Document doc){
        Elements lisOld = doc.select("#detail-bullets > table > tbody > tr > td > div > ul > li");
        Elements lisNew = doc.select("#a-page > div.av-page-desktop.avu-retail-page > div.avu-content.avu-section > div > div > div.DVWebNode-detail-atf-wrapper.DVWebNode > div.av-detail-section > div > div._2vWb4y.dv-dp-node-meta-info > div > div");
        if(lisOld.size() == 0 && lisNew.size() == 0){
            return false;
        }
        String htmlBaseDirPath = baseDir + "/htmls";
        File htmlBaseDir = new File(htmlBaseDirPath);
        if(!htmlBaseDir.exists()){
            htmlBaseDir.mkdir();
        }
        try{
            File htmlFile = new File(htmlBaseDirPath + "/" + productId + ".html");
            if(!htmlFile.exists()) {
                if(!htmlFile.createNewFile()){
                    throw new Exception(htmlBaseDirPath + "/" + productId + ".html" + " 创建失败.");
                }
            }
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(htmlFile));
            bufferedWriter.write(doc.html());
            bufferedWriter.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }



    private static String[] processActors(Element element){
        Elements elements = element.children();
        String[] actors = new String[elements.size() - 1];
        for (int i = 1; i < elements.size(); i++) {
            actors[i-1] = elements.get(i).html();
        }
        return actors;
    }

    private static String processDirector(Element element){
        Elements elements = element.children();
        return elements.size() > 1 ? elements.get(1).html().replaceAll(",", " ") : "";
    }

    private static String[] processFormat(Element element){
        String formats = element.text().split("Format: ")[1];
        return formats.split(", ");
    }

    private static String processReleaseDate(Element element){
        return element.text().split("Date: ")[1].replaceAll(",", " ");
    }

    private static String processRunTime(Element element){
        return element.text().split("Time: ")[1];
    }


    public static void main(String[] args) {
        MovieInfoCrawler crawler = new MovieInfoCrawler();
        Document document = crawler.crawlOneProduct("B009W02BXS", false);
        System.out.println(document);
        parseAndSaveDoc("B009VMQR3W", document);
//        System.out.println(bundle.getProductActors());
//        System.out.println(bundle.getProductDetail());
//        System.out.println(bundle.getProductFormats());
    }
}
