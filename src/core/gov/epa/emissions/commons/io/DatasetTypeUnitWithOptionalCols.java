package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.io.importer.TableFormatWithOptionalCols;

public class DatasetTypeUnitWithOptionalCols implements FormatUnit {

    private TableFormatWithOptionalCols tableFormat;

    private FileFormatWithOptionalCols fileFormat;

    private boolean required;

    private InternalSource internalSource;

    public DatasetTypeUnitWithOptionalCols(TableFormatWithOptionalCols tableFormat,
            FileFormatWithOptionalCols fileFormat) {
        this.tableFormat = tableFormat;
        this.fileFormat = fileFormat;
        this.required = false;
    }

    public FileFormat fileFormat() {
        return fileFormat;
    }

    public TableFormat tableFormat() {
        return tableFormat;
    }

    public boolean isRequired() {
        return required;
    }

    public void setInternalSource(InternalSource internalSource) {
        this.internalSource = internalSource;
    }

    public InternalSource getInternalSource() {
        return internalSource;
    }

}
