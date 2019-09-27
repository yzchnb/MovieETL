package MovieInfoCrawl;

import Entities.ProductActor;
import Entities.ProductBundle;
import Entities.ProductDetail;
import Entities.ProductFormat;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayList;

public class MovieInfoTransformer {
    public static ProductBundle ParseDoc(String productId, Document doc){
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
        return elements.size() > 1 ? elements.get(1).html() : "";
    }

    private static String[] processFormat(Element element){
        String formats = element.text().split("Format: ")[1];
        return formats.split(", ");
    }

    private static String processReleaseDate(Element element){
        return element.text().split("Date: ")[1];
    }

    private static String processRunTime(Element element){
        return element.text().split("Time: ")[1];
    }


    public static void main(String[] args) {
        MovieInfoCrawler crawler = new MovieInfoCrawler();
        Document document = crawler.crawlOneProduct("0001489305");
        System.out.println(document);
        ProductBundle bundle = ParseDoc("0001489305", document);
        System.out.println(bundle.getProductActors());
        System.out.println(bundle.getProductDetail());
        System.out.println(bundle.getProductFormats());
    }
}
