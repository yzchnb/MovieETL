package Entities;

import java.util.ArrayList;

public class ProductBundle {
    private ProductDetail productDetail;
    private ArrayList<ProductActor> productActors;

    public ProductBundle(ProductDetail productDetail, ArrayList<ProductActor> productActors, ArrayList<ProductFormat> productFormats) {
        this.productDetail = productDetail;
        this.productActors = productActors;
        this.productFormats = productFormats;
    }

    public ProductDetail getProductDetail() {
        return productDetail;
    }

    public void setProductDetail(ProductDetail productDetail) {
        this.productDetail = productDetail;
    }

    public ArrayList<ProductActor> getProductActors() {
        return productActors;
    }

    public void setProductActors(ArrayList<ProductActor> productActors) {
        this.productActors = productActors;
    }

    public ArrayList<ProductFormat> getProductFormats() {
        return productFormats;
    }

    public void setProductFormats(ArrayList<ProductFormat> productFormats) {
        this.productFormats = productFormats;
    }

    private ArrayList<ProductFormat> productFormats;

}
