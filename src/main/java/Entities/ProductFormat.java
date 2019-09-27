package Entities;

public class ProductFormat {
    private String productId;
    private String format;

    public ProductFormat(String productId, String format) {
        this.productId = productId;
        this.format = format;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }
}
