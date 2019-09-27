package Entities;

public class ProductActor {
    private String productId;
    private String actorName;

    public ProductActor(String productId, String actorName) {
        this.productId = productId;
        this.actorName = actorName;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getActorName() {
        return actorName;
    }

    public void setActorName(String actorName) {
        this.actorName = actorName;
    }
}
