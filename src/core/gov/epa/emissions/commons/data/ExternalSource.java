package gov.epa.emissions.commons.data;

import java.io.Serializable;

public class ExternalSource implements Serializable {

    private String datasource;

    private long listindex;

    public long getListindex() {
        return listindex;
    }

    public void setListindex(long listindex) {
        this.listindex = listindex;
    }

    public ExternalSource() {// dummy: needed by Hibernate
    }

    public ExternalSource(String datasource) {
        this.datasource = datasource;
    }

    public String getDatasource() {
        return datasource;
    }

    public void setDatasource(String datasource) {
        this.datasource = datasource;
    }

}
