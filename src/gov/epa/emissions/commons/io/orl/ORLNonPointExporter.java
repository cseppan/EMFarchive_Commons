package gov.epa.emissions.commons.io.orl;

import java.io.File;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.NewExporter;
import gov.epa.emissions.commons.io.exporter.orl.ExporterException;

public class ORLNonPointExporter implements NewExporter {

    private NewORLExporter delegate;

    public ORLNonPointExporter(Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes) {
        ORLColumnsMetadata colsMetadata = new OrlNonPointColumnsMetadata(sqlDataTypes);
        delegate = new NewORLExporter(dataset, datasource, colsMetadata);
    }

    public void export(File file) throws ExporterException {
        delegate.export(file);
    }

}
