package gov.epa.emissions.commons.io;

import java.io.Serializable;

public class Keyword implements Serializable {

    private long id;

    private String name;

    public Keyword() {// dummy: needed by Hibernate
    }

    public Keyword(String name) {
        this.name = name;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean equals(Object other) {
        if (!(other instanceof Keyword))
            return false;

        return name.equals(((Keyword) other).name);
    }

    public int hashCode() {
        return (int) id;
    }

}
