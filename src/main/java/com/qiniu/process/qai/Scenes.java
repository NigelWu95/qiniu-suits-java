package com.qiniu.process.qai;

public enum Scenes {

    PULP(1, "pulp"),

    TERROR(2, "terror"),

    POLITICIAN(3, "politician"),

    PULP_TERROR(4, "pulp_terror"),

    TERROR_POLITICIAN(5, "terror_politician"),

    PULP_TERROR_POLITICIAN(6, "pulp_terror_politician");

    private Integer order;
    private String des;

    Scenes(int i, String des) {
        this.order = i;
        this.des = des;
    }

    public int getOrder() {
        return this.order;
    }


    public String getJson() {
        return this.des;
    }
}
