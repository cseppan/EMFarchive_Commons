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
    
    /**
     * 
     */
    public Keyword() {
        super();
    }

    public Keyword(String name) {
        super();
        this.name=name;
    }

    /**
     * @return Returns the id.
     */
    public long getId() {
        return id;
    }

    /**
     * @param id The id to set.
     */
    public void setId(long id) {
        this.id = id;
    }

    /**
     * @return Returns the name.
     */
    public String getName() {
        return name;
    }

    /**
     * @param name The name to set.
     */
    public void setName(String name) {
        this.name = name;
    }

}
