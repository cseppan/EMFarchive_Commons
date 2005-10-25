package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.io.importer.TableFormatWithOptionalCols;

public class DatasetTypeUnitWithOptionalCols {

    private TableFormatWithOptionalCols tableMetadata;

    private FileFormatWithOptionalCols fileMetadata;

    public DatasetTypeUnitWithOptionalCols(TableFormatWithOptionalCols tableFormat,
            FileFormatWithOptionalCols fileFormat) {
        this.tableMetadata = tableFormat;
        this.fileMetadata = fileFormat;
    }

    public FileFormatWithOptionalCols fileFormat() {
        return fileMetadata;
    }

    public TableFormatWithOptionalCols tableFormat() {
        return tableMetadata;
    }

}
