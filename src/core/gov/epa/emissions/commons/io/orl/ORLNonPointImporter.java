package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.TableFormat;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.importer.FillDefaultValues;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.temporal.FixedColsTableFormat;

import java.io.File;

public class ORLNonPointImporter implements Importer {

    private ORLImporter delegate;

    public ORLNonPointImporter(File folder, String[] filePatterns, Dataset dataset, Datasource datasource,
            SqlDataTypes sqlDataTypes) throws ImporterException {
        validateFile(filePatterns);
        File file = new File(folder, filePatterns[0]);

        FileFormatWithOptionalCols fileFormat = fileFormat(sqlDataTypes);
        TableFormat tableFormat = new FixedColsTableFormat(fileFormat(sqlDataTypes), sqlDataTypes);

        create(file, dataset, datasource, fileFormat, tableFormat);
    }

    private void validateFile(String[] filePatterns) throws ImporterException {
        if (filePatterns.length > 1) {
            throw new ImporterException("Too many parameters for importer: ORL Non Point "
                    + " requires only one file pattern or filename");
        }
        if (filePatterns[0].length() == 0) {
            throw new ImporterException("ORL Non Point importer requires a filename");
        }
    }

    public ORLNonPointImporter(File file, Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes,
            DataFormatFactory factory) {
        FileFormatWithOptionalCols fileFormat = fileFormat(sqlDataTypes, factory.defaultValuesFiller());
        TableFormat tableFormat = factory.tableFormat(fileFormat, sqlDataTypes);

        create(file, dataset, datasource, fileFormat, tableFormat);
    }

    private FileFormatWithOptionalCols fileFormat(SqlDataTypes sqlDataTypes, FillDefaultValues filler) {
        return new ORLNonPointFileFormat(sqlDataTypes, filler);
    }

    private void create(File file, Dataset dataset, Datasource datasource, FileFormatWithOptionalCols fileFormat,
            TableFormat tableFormat) {
        DatasetTypeUnit formatUnit = new DatasetTypeUnit(tableFormat, fileFormat);
        delegate = new ORLImporter(file, dataset, formatUnit, datasource);
    }

    private FileFormatWithOptionalCols fileFormat(SqlDataTypes sqlDataTypes) {
        return new ORLNonPointFileFormat(sqlDataTypes);
    }

    public void run() throws ImporterException {
        delegate.run();
    }
}
