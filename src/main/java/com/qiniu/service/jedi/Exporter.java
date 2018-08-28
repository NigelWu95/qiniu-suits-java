package com.qiniu.service.jedi;

public class Exporter extends ExportBaseModel {

   private String name = "";
   private String status = "";

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String toString() {
        return this.name + "," + this.status + "," + super.toString();
    }
}