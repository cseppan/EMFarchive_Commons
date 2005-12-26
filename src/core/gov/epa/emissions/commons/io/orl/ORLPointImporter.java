package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.TableFormat;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.temporal.FixedColsTableFormat;

import java.io.File;

public class ORLPointImporter implements Importer {

    private ORLImporter delegate;

    public ORLPointImporter(File file, Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes) {
        FileFormatWithOptionalCols fileFormat = new ORLPointFileFormat(sqlDataTypes);
        TableFormat tableFormat = new FixedColsTableFormat(fileFormat, sqlDataTypes);

        create(file, dataset, datasource, fileFormat, tableFormat);
    }

    public ORLPointImporter(File file, Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes,
            DataFormatFactory factory) {
        FileFormatWithOptionalCols fileFormat = new ORLPointFileFormat(sqlDataTypes, factory.defaultValuesFiller());
        TableFormat tableFormat = factory.tableFormat(fileFormat, sqlDataTypes);

        create(file, dataset, datasource, fileFormat, tableFormat);
    }

    private void create(File file, Dataset dataset, Datasource datasource, FileFormatWithOptionalCols fileFormat,
            TableFormat tableFormat) {
        DatasetTypeUnit formatUnit = new DatasetTypeUnit(tableFormat, fileFormat);
        delegate = new ORLImporter(file, dataset, formatUnit, datasource);
    }

    public void run() throws ImporterException {
        delegate.run();
    }

}
