package Entities;

public class ProductDetail {
    private String productId;
    private String releaseDate;
    private String director;
    private String runTime;

    public ProductDetail(String productId, String releaseDate, String director, String length) {
        this.productId = productId;
        this.releaseDate = releaseDate;
        this.director = director;
        this.runTime = length;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getReleaseDate() {
        return releaseDate;
    }

    public void setReleaseTime(String releaseDate) {
        this.releaseDate = releaseDate;
    }

    public String getDirector() {
        return director;
    }

    public void setDirector(String director) {
        this.director = director;
    }

    public String getRunTime() {
        return runTime;
    }

    public void setRunTime(String length) {
        this.runTime = length;
    }
}
