package gov.epa.emissions.commons.io.ida;

import java.sql.ResultSet;
import java.sql.SQLException;

import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.generic.GenericExporter;

public class IDANonPointNonRoadExporter extends GenericExporter {

    private FileFormat fileFormat;
    
    public IDANonPointNonRoadExporter(Dataset dataset, DbServer dbServer, FileFormat fileFormat) {
        super(dataset, dbServer, fileFormat);
        setup(fileFormat, "");
    }

    public IDANonPointNonRoadExporter(Dataset dataset, DbServer dbServer, FileFormat fileFormat,
            DataFormatFactory dataFormatFactory) {
        super(dataset, dbServer, fileFormat, dataFormatFactory);
        setup(fileFormat, "");
    }
    
    private void setup(FileFormat fileFormat, String delimiter) {
        this.fileFormat = fileFormat;
        setDelimiter(delimiter);
    }
    
    protected String formatValue(String[] cols, int index, ResultSet data) throws SQLException {
        int fileIndex = index;
        if (isTableVersioned(cols))
            fileIndex = index - 3;

        Column column = fileFormat.cols()[fileIndex - 4];
        return getFixedPositionValue(column, data);
    }
    
    // Due to two more columns added to dataset during import
    protected int startCol(String[] cols) {
        int i = 4;
        if (isTableVersioned(cols))
            i = 7;

        return i;
    }

}
