package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnitWithOptionalCols;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.TableFormatWithOptionalCols;

import java.io.File;

public class ORLPointImporter implements Importer {

    private ORLImporter delegate;
    
    private SqlDataTypes sqlDataTypes;
    
    public ORLPointImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
        this.sqlDataTypes = sqlDataTypes;
        delegate = new ORLImporter(datasource);
    }

    public void run(Dataset dataset) throws ImporterException {
        FileFormatWithOptionalCols fileFormat = new ORLPointFileFormat(sqlDataTypes);
        TableFormatWithOptionalCols tableColsMetadata = new TableFormatWithOptionalCols(fileFormat, sqlDataTypes);
        DatasetTypeUnitWithOptionalCols unit = new DatasetTypeUnitWithOptionalCols(tableColsMetadata, fileFormat);

        delegate.run(dataset,unit);
    }

    public void preCondition(File folder, String filePattern) {
        delegate.preCondition(folder, filePattern);
    }

}
