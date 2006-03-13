package gov.epa.emissions.commons.data;

import gov.epa.emissions.commons.security.User;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class DatasetType implements Serializable, Lockable, Comparable {

    private long id;

    private String name;

    private String description;

    private int minFiles;

    private int maxFiles;

    private boolean external;

    private String defaultSortOrder;

    private String importerClassName;

    private String exporterClassName;

    private Mutex lock;

    private List keyValsList;

    private List qaStepTemplates;

    public DatasetType() {
        keyValsList = new ArrayList();
        qaStepTemplates = new ArrayList();
        lock = new Mutex();
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

    public int getMaxFiles() {
        return maxFiles;
    }

    public void setMaxFiles(int maxfiles) {
        this.maxFiles = maxfiles;
    }

    public int getMinFiles() {
        return minFiles;
    }

    public void setMinFiles(int minfiles) {
        this.minFiles = minfiles;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String toString() {
        return getName();
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public int hashCode() {
        return name.hashCode();
    }

    public boolean equals(Object other) {
        return (other instanceof DatasetType && ((DatasetType) other).id == id);
    }

    public boolean isExternal() {
        return external;
    }

    public void setExternal(boolean external) {
        this.external = external;
    }

    public String getLockOwner() {
        return lock.getLockOwner();
    }

    public void setLockOwner(String username) {
        lock.setLockOwner(username);
    }

    public Date getLockDate() {
        return lock.getLockDate();
    }

    public void setLockDate(Date lockDate) {
        this.lock.setLockDate(lockDate);
    }

    public boolean isLocked(String owner) {
        return lock.isLocked(owner);
    }

    public boolean isLocked(User owner) {
        return lock.isLocked(owner);
    }

    public boolean isLocked() {
        return lock.isLocked();
    }

    public String getDefaultSortOrder() {
        return defaultSortOrder;
    }

    public void setDefaultSortOrder(String defaultSortOrder) {
        this.defaultSortOrder = defaultSortOrder;
    }

    public int compareTo(Object o) {
        return name.compareTo(((DatasetType) o).getName());
    }

    public KeyVal[] getKeyVals() {
        return (KeyVal[]) keyValsList.toArray(new KeyVal[0]);
    }

    public void setKeyVals(KeyVal[] keyvals) {
        keyValsList.clear();
        keyValsList.addAll(Arrays.asList(keyvals));
    }

    public void addKeyVal(KeyVal val) {
        keyValsList.add(val);
    }

    public void setQaStepTemplates(QAStepTemplate[] templates) {
        qaStepTemplates.clear();
        qaStepTemplates.addAll(Arrays.asList(templates));
    }

    public void addQaStepTemplate(QAStepTemplate val) {
        qaStepTemplates.add(val);
    }

    public QAStepTemplate[] getQaStepTemplates() {
        return (QAStepTemplate[]) qaStepTemplates.toArray(new QAStepTemplate[0]);
    }
}
