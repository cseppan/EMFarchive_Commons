package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.temporal.TableFormat;

public class DatasetTypeUnit implements FormatUnit {

    private TableFormat tableFormat;

    private FileFormat fileFormat;

    public DatasetTypeUnit(TableFormat tableFormat, FileFormat fileFormat) {
        this.tableFormat = tableFormat;
        this.fileFormat = fileFormat;
    }

    public FileFormat fileFormat() {
        return fileFormat;
    }

    public TableFormat tableFormat() {
        return tableFormat;
    }

}
