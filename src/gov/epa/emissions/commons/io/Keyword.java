/*
 * Creation on Oct 26, 2005
 * Eclipse Project Name: EMF
 * File Name: Keyword.java
 * Author: Conrad F. D'Cruz
 */
/**
 * 
 */

package gov.epa.emissions.commons.io;

import java.io.Serializable;

/**
 * @author Conrad F. D'Cruz
 * 
 */
public class Keyword implements Serializable {

    private long id;

    private String name;

    public Keyword() {
        super();
    }

    public Keyword(String name) {
        super();
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
