package gov.epa.emissions.commons.data;

import gov.epa.emissions.commons.security.User;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class DatasetType implements Serializable, Lockable, Comparable {

    private int id;

    private String name;

    private String description;

    private int minFiles;

    private int maxFiles;

    private boolean external;

    private int tablePerDataset;

    private String defaultSortOrder;

    private String importerClassName;

    private String exporterClassName;

    private Mutex lock;

    private KeyVal[] keyValsList = new KeyVal[]{};

    private QAStepTemplate[] qaStepTemplates = new QAStepTemplate[]{};

    public static final String orlMergedInventory = "ORL Merged Inventory";

    public static final String orlNonpointInventory = "ORL Nonpoint Inventory (ARINV)";

    public static final String orlPointInventory = "ORL Point Inventory (PTINV)";

    public static final String orlNonroadInventory = "ORL Nonroad Inventory (ARINV)";

    public static final String orlOnroadInventory = "ORL Onroad Inventory (MBINV)";

    public static final String projectionPacket = "Projection Packet";

    public static final String allowablePacket = "Allowable Packet";

    public static final String controlPacket = "Control Packet";
    
    public static final String strategyDetailedResult = "Control Strategy Detailed Result";

    public static final String strategyCountySummary = "Strategy County Summary";
    
    public static final String strategyImpactSummary = "Strategy Impact Summary";

    public static final String strategyMeasureSummary = "Strategy Measure Summary";

    public static final String rsmPercentReduction = "RSM Percent Reduction";

    public DatasetType() {
//        keyValsList = new ArrayList();
//        qaStepTemplates = new ArrayList();
        lock = new Mutex();
    }

    public DatasetType(int id, String name) {
        this();
        this.id = id;
        this.name = name;
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

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int hashCode() {
        return name.hashCode();
    }

    public boolean equals(Object other) {
        return (other instanceof DatasetType && ((DatasetType) other).getId() == id);
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
        return this.keyValsList;
    }

    public void setKeyVals(KeyVal[] keyvals) {
        this.keyValsList = keyvals;
    }

    public void addKeyVal(KeyVal val) {
        List<KeyVal> keyVals = new ArrayList<KeyVal>();
        keyVals.addAll(Arrays.asList(this.keyValsList));
        keyVals.add(val);
        
        this.keyValsList = keyVals.toArray(new KeyVal[0]);
    }

    public void setQaStepTemplates(QAStepTemplate[] templates) {
        this.qaStepTemplates = templates;
    }

    public void addQaStepTemplate(QAStepTemplate template) {
        List<QAStepTemplate> templates = new ArrayList<QAStepTemplate>();
        templates.addAll(Arrays.asList(this.qaStepTemplates));
        templates.add(template);
        
        this.qaStepTemplates = templates.toArray(new QAStepTemplate[0]);
    }

    public void removeQaStepTemplate(QAStepTemplate template) {
        List<QAStepTemplate> templates = new ArrayList<QAStepTemplate>();
        templates.addAll(Arrays.asList(this.qaStepTemplates));
        for (int i = 0; i < templates.size(); i++) {
            if (template.getName().equals(templates.get(i).getName())) templates.remove(i);
        }
        this.qaStepTemplates = templates.toArray(new QAStepTemplate[0]);
    }

    public QAStepTemplate[] getQaStepTemplates() {
        return this.qaStepTemplates;
    }

    public int getTablePerDataset() {
        return tablePerDataset;
    }

    public void setTablePerDataset(int tablePerDataset) {
        this.tablePerDataset = tablePerDataset;
    }

}
