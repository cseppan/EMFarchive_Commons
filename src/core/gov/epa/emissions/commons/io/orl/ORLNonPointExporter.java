package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Exporter;
import gov.epa.emissions.commons.io.ExporterException;
import gov.epa.emissions.commons.io.FileFormat;

import java.io.File;

public class ORLNonPointExporter implements Exporter {

    private ORLExporter delegate;

    public ORLNonPointExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes) {
        delegate = new ORLExporter(dataset, dbServer, fileFormat(sqlDataTypes));
    }

    public ORLNonPointExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes,
            DataFormatFactory formatFactory) {
        delegate = new ORLExporter(dataset, dbServer, fileFormat(sqlDataTypes), formatFactory);
    }

    private FileFormat fileFormat(SqlDataTypes sqlDataTypes) {
        return new ORLNonPointFileFormat(sqlDataTypes);
    }

    public void export(File file) throws ExporterException {
        delegate.export(file);
    }

}
