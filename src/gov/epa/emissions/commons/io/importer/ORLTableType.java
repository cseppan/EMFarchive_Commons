package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.io.DatasetType;

public class ORLTableType {

    private DatasetType datasetType;

    private String baseType;

    public ORLTableType(DatasetType datasetType) {
        this.datasetType = datasetType;
        this.baseType = datasetType.getName();
    }

    public DatasetType datasetType() {
        return datasetType;
    }

    public String summaryType() {
        return baseType + " Summary";
    }

    public String baseType() {
        return baseType;
    }

}