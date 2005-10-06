package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.io.DatasetType;

public class ORLTableType {

    private DatasetType datasetType;

    private String baseType;

    ORLTableType(DatasetType datasetType) {
        this.datasetType = datasetType;
        this.baseType = datasetType.getName();
    }

    public DatasetType datasetType() {
        return datasetType;
    }

    public String summary() {
        return baseType + " Summary";
    }

    public String base() {
        return baseType;
    }

    public boolean equals(Object other) {
        if(!(other instanceof ORLTableType))
            return false;
        
        return datasetType.equals(((ORLTableType) other).datasetType);
    }
}