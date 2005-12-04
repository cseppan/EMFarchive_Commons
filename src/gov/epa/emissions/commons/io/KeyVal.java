package gov.epa.emissions.commons.io;

import java.io.Serializable;

public class KeyVal implements Serializable {

    private long id;

    private Keyword keyword;

    private String value;

    private long listindex;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Keyword getKeyword() {
        return keyword;
    }

    public void setKeyword(Keyword keyword) {
        this.keyword = keyword;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public long getListindex() {
        return listindex;
    }

    public void setListindex(long listindex) {
        this.listindex = listindex;
    }

}
