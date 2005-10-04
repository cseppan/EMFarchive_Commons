package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.io.DatasetType;

public class ORLTableType {

    private DatasetType datasetType;

    private String summaryType;

    private String baseType;

    public ORLTableType(DatasetType datasetType, String baseType, String summaryType) {
        this.datasetType = datasetType;
        this.baseType = baseType;
        this.summaryType = summaryType;
    }

    public DatasetType datasetType() {
        return datasetType;
    }

    public String summaryType() {
        return summaryType;
    }

    public String baseType() {
        return baseType;
    }

}