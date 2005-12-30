package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.security.User;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class DatasetType implements Serializable {

    private long datasettypeid;

    private String name;

    private String description;

    private int minfiles;

    private int maxfiles;

    private boolean external;

    private List keywordsList;

    private String importerClassName;

    private String exporterClassName;

    private Mutex lock;

    public DatasetType() {
        this.keywordsList = new ArrayList();
        this.lock = new Mutex();
    }

    public DatasetType(String name) {
        this();
        this.name = name;
    }

    public String getExporterClassName() {
        return exporterClassName;
    }

    public void setExporterClassName(String exporterClassName) {
        this.exporterClassName = exporterClassName;
    }

    public String getImporterClassName() {
        return importerClassName;
    }

    public void setImporterClassName(String importerClassName) {
        this.importerClassName = importerClassName;
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

    public boolean isExternal() {
        return external;
    }

    public void setExternal(boolean external) {
        this.external = external;
    }

    public void addKeyword(Keyword keyword) {
        keywordsList.add(keyword);
    }

    public Keyword[] getKeywords() {
        return (Keyword[]) keywordsList.toArray(new Keyword[0]);
    }

    public void setKeywords(Keyword[] keywords) {
        keywordsList.clear();
        keywordsList.addAll(Arrays.asList(keywords));
    }

    public String getUsername() {
        return lock.getUsername();
    }

    public void setUsername(String username) {
        lock.setUsername(username);
    }

    public Date getLockDate() {
        return lock.getLockDate();
    }

    public void setLockDate(Date lockDate) {
        this.lock.setLockDate(lockDate);
    }

    public boolean isLocked(User user) {
        return lock.isLocked(user);
    }

    public boolean isLocked() {
        return lock.isLocked();
    }
}
