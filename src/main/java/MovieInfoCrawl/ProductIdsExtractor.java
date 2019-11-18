package MovieInfoCrawl;

import java.util.ArrayList;

public interface ProductIdsExtractor {
    ArrayList<String> getProductIdsForRange(int startFrom, int limitation);
    int getProductIdsCount();
    String getProductId(int index);
}
