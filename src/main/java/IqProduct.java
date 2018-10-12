import java.io.Serializable;

public  class  IqProduct implements Serializable {
    private int sort;
    private int productName;

    public int getSort() {
        return sort;
    }

    public void setSort(int sort) {
        this.sort = sort;
    }

    public int getProductName() {
        return productName;
    }

    public void setProductName(int productName) {
        this.productName = productName;
    }
}