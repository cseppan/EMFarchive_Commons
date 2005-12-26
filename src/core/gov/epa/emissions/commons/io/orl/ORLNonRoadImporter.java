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

public class ORLNonRoadImporter implements Importer {

    private ORLImporter delegate;

    public ORLNonRoadImporter(File file, Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes)
            throws ImporterException {
        FileFormatWithOptionalCols fileFormat = new ORLNonRoadFileFormat(sqlDataTypes);
        TableFormat tableFormat = new FixedColsTableFormat(fileFormat, sqlDataTypes);

        create(file, dataset, datasource, fileFormat, tableFormat);
    }

    public ORLNonRoadImporter(File file, Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes,
            DataFormatFactory factory) throws ImporterException {
        FileFormatWithOptionalCols fileFormat = new ORLNonRoadFileFormat(sqlDataTypes, factory.defaultValuesFiller());
        TableFormat tableFormat = factory.tableFormat(fileFormat, sqlDataTypes);

        create(file, dataset, datasource, fileFormat, tableFormat);
    }

    private void create(File file, Dataset dataset, Datasource datasource, FileFormatWithOptionalCols fileFormat,
            TableFormat tableFormat) throws ImporterException {
        DatasetTypeUnit formatUnit = new DatasetTypeUnit(tableFormat, fileFormat);
        delegate = new ORLImporter(dataset, formatUnit, datasource);
        delegate.setup(file);
    }

    public void run() throws ImporterException {
        delegate.run();
    }

}
