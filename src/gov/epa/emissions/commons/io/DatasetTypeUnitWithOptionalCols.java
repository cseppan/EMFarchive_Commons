package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.TableFormatWithOptionalCols;
import gov.epa.emissions.commons.io.importer.temporal.TableFormat;

public class DatasetTypeUnitWithOptionalCols implements FormatUnit{

    private TableFormatWithOptionalCols tableMetadata;

    private FileFormatWithOptionalCols fileMetadata;

    public DatasetTypeUnitWithOptionalCols(TableFormatWithOptionalCols tableFormat,
            FileFormatWithOptionalCols fileFormat) {
        this.tableMetadata = tableFormat;
        this.fileMetadata = fileFormat;
    }

    public FileFormat fileFormat() {
        return fileMetadata;
    }

    public TableFormat tableFormat() {
        return tableMetadata;
    }

}
