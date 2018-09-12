package com.xian.liankloud.domain;


/*
category
 "brand, "+
  "count, "+
 */
public class AdProvinceTop3
{
    private String category;
    private String brand;
    private long click_count;

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public long getClick_count() {
        return click_count;
    }

    public void setClick_count(long click_count) {
        this.click_count = click_count;
    }
}
