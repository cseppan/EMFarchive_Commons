package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.FixedColsTableFormat;
import gov.epa.emissions.commons.io.TableFormat;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.importer.FillDefaultValues;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

public class ORLNonPointImporter implements Importer {

    private ORLImporter delegate;

    public ORLNonPointImporter(File folder, String[] filenames, Dataset dataset, Datasource datasource,
            SqlDataTypes sqlDataTypes) throws ImporterException {
        FileFormatWithOptionalCols fileFormat = fileFormat(sqlDataTypes);
        TableFormat tableFormat = new FixedColsTableFormat(fileFormat(sqlDataTypes), sqlDataTypes);

        create(folder, filenames, dataset, datasource, fileFormat, tableFormat);
    }

    public ORLNonPointImporter(File folder, String[] filenames, Dataset dataset, Datasource datasource,
            SqlDataTypes sqlDataTypes, DataFormatFactory factory) throws ImporterException {
        FileFormatWithOptionalCols fileFormat = fileFormat(sqlDataTypes, factory.defaultValuesFiller());
        TableFormat tableFormat = factory.tableFormat(fileFormat, sqlDataTypes);

        create(folder, filenames, dataset, datasource, fileFormat, tableFormat);
    }

    private FileFormatWithOptionalCols fileFormat(SqlDataTypes sqlDataTypes, FillDefaultValues filler) {
        return new ORLNonPointFileFormat(sqlDataTypes, filler);
    }

    private void create(File folder, String[] filePatterns, Dataset dataset, Datasource datasource,
            FileFormatWithOptionalCols fileFormat, TableFormat tableFormat) throws ImporterException {
        DatasetTypeUnit formatUnit = new DatasetTypeUnit(tableFormat, fileFormat);
        delegate = new ORLImporter(folder, filePatterns, dataset, formatUnit, datasource);
    }

    private FileFormatWithOptionalCols fileFormat(SqlDataTypes sqlDataTypes) {
        return new ORLNonPointFileFormat(sqlDataTypes);
    }

    public void run() throws ImporterException {
        delegate.run();
    }
}
