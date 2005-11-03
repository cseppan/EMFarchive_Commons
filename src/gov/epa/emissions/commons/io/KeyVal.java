/*
 * Creation on Oct 26, 2005
 * Eclipse Project Name: EMF
 * File Name: KeyVal.java
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
public class KeyVal implements Serializable {

    private long id;

    private Keyword keyword;

    private String value;

    private long listindex;

    /**
     * 
     */
    public KeyVal() {
        super();
    }

    /**
     * @return Returns the id.
     */
    public long getId() {
        return id;
    }

    /**
     * @param id
     *            The id to set.
     */
    public void setId(long id) {
        this.id = id;
    }

    /**
     * @return Returns the keyword.
     */
    public Keyword getKeyword() {
        return keyword;
    }

    /**
     * @param keyword
     *            The keyword to set.
     */
    public void setKeyword(Keyword keyword) {
        this.keyword = keyword;
    }

    /**
     * @return Returns the value.
     */
    public String getValue() {
        return value;
    }

    /**
     * @param value
     *            The value to set.
     */
    public void setValue(String value) {
        this.value = value;
    }

    /**
     * @return Returns the listindex.
     */
    public long getListindex() {
        return listindex;
    }

    /**
     * @param listindex
     *            The listindex to set.
     */
    public void setListindex(long listindex) {
        this.listindex = listindex;
    }

}
