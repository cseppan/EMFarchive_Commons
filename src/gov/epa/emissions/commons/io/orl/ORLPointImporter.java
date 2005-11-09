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
    
    public ORLPointImporter(Datasource datasource, SqlDataTypes sqlDataTypes, Dataset dataset) {
        
        FileFormatWithOptionalCols fileFormat = new ORLPointFileFormat(sqlDataTypes);
        TableFormatWithOptionalCols tableColsMetadata = new TableFormatWithOptionalCols(fileFormat, sqlDataTypes);
        DatasetTypeUnitWithOptionalCols formatUnit = new DatasetTypeUnitWithOptionalCols(tableColsMetadata, fileFormat);
        
        delegate = new ORLImporter(dataset, formatUnit, datasource);
    }
    
    public void preCondition(File folder, String filePattern) throws Exception {
        delegate.preCondition(folder, filePattern);
    }

    public void run(Dataset dataset) throws ImporterException {
        delegate.run();
    }

}
