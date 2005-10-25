package gov.epa.emissions.commons.io.importer.ida;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.temporal.FixedColsTableFormat;

public class IDAActivityImporter {

    private IDAImporter delegate;

    private SqlDataTypes sqlDataTypes;

    public IDAActivityImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
        this.sqlDataTypes = sqlDataTypes;
        delegate = new IDAImporter(datasource, sqlDataTypes);
    }

    public void run(File file, Dataset dataset) throws ImporterException {
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(file));
            IDAHeaderReader headerReader = new IDAHeaderReader(reader);
            headerReader.read();
            
            FileFormat fileFormat = new IDAActivityFileFormat(headerReader.polluntants(), sqlDataTypes);
            FixedColsTableFormat tableFormat = new FixedColsTableFormat(fileFormat, sqlDataTypes);
            DatasetTypeUnit unit = new DatasetTypeUnit(tableFormat, fileFormat);

            delegate.run(reader, unit, headerReader.comments(), dataset);

        } catch (Exception e) {
            throw new ImporterException("could not import File - " + file.getAbsolutePath() + " into Dataset - "
                    + dataset.getName());
        }

    }

}
