package gov.epa.emissions.commons.io;

import java.io.Serializable;

public class DatasetType implements Serializable {

    /**
     * Generated serialVersionUID
     */
    private static final long serialVersionUID = 4694789007596096797L;

    private long datasettypeid;

    private String name;

    private String description;

    private int minfiles;

    private int maxfiles;

    private String uid = null;

    // FIXME: add the min/max cols as cols in table
    private int minColumns;

    private int maxColumns;

    /**
     * @return Returns the uid.
     */
    public String getUid() {
        return uid;
    }

    /**
     * @param uid
     *            The uid to set.
     */
    public void setUid(String uid) {
        this.uid = uid;
    }

    /**
     * No argument constructor needed for hibernate bean mapping
     * 
     */
    public DatasetType() {
        super();
    }

    public DatasetType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int getMaxfiles() {
        return maxfiles;
    }

    public void setMaxfiles(int maxfiles) {
        this.maxfiles = maxfiles;
    }

    public int getMinfiles() {
        return minfiles;
    }

    public void setMinfiles(int minfiles) {
        this.minfiles = minfiles;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String toString() {
        return getName();
    }

    public long getDatasettypeid() {
        return datasettypeid;
    }

    public void setDatasettypeid(long datasettypeid) {
        this.datasettypeid = datasettypeid;
    }

    public int hashCode() {
        return name.hashCode();
    }

    public boolean equals(Object other) {
        return (other instanceof DatasetType && ((DatasetType) other).name.equals(name));
    }

    // TODO: do the min/max cols apply to all Dataset Types ? or just ORL ?
    public void setMinColumns(int minCols) {
        this.minColumns = minCols;
    }

    public void setMaxColumns(int maxCols) {
        this.maxColumns = maxCols;
    }

    public int getMinColumns() {
        return minColumns;
    }

    public int getMaxColumns() {
        return maxColumns;
    }
}
