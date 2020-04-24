package com.itsaur.meetup.kafka.products.views;

import java.time.OffsetDateTime;

public class ProductView {
    private String product;
    private String user;
    private OffsetDateTime time;

    public ProductView() {
    }

    public ProductView(String product, String user, OffsetDateTime time) {
        this.product = product;
        this.user = user;
        this.time = time;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public OffsetDateTime getTime() {
        return time;
    }

    public void setTime(OffsetDateTime time) {
        this.time = time;
    }
}
