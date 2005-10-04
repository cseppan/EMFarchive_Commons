package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.io.DatasetType;

public class TableType {

    private DatasetType datasetType;

    private String summaryType;

    private String[] baseTypes;

    public TableType(DatasetType datasetType, String[] baseTypes, String summaryType) {
        this.datasetType = datasetType;
        this.baseTypes = baseTypes;
        this.summaryType = summaryType;
    }

    public DatasetType datasetType() {
        return datasetType;
    }

    public String summaryType() {
        return summaryType;
    }

    public String[] baseTypes() {
        return baseTypes;
    }

}