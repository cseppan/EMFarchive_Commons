package gov.epa.emissions.commons.io.importer.ida;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.temporal.FixedColsTableFormat;

import java.io.BufferedReader;
import java.io.FileReader;

public class IDAAreaImporter {
    private IDAImporter delegate;

    private SqlDataTypes sqlDataTypes;

    public IDAAreaImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
        this.sqlDataTypes = sqlDataTypes;
        delegate = new IDAImporter(datasource);
    }

    public void run(Dataset dataset) throws ImporterException {
        BufferedReader reader;
        InternalSource internalSource = dataset.getInternalSources()[0];
        String source = internalSource.getSource();
        try {
            reader = new BufferedReader(new FileReader(source));
            IDAHeaderReader headerReader = new IDAHeaderReader(reader);
            headerReader.read();
            
            FileFormat fileFormat = new IDAAreaFileFormat(headerReader.polluntants(), sqlDataTypes);
            FixedColsTableFormat tableFormat = new FixedColsTableFormat(fileFormat, sqlDataTypes);
            DatasetTypeUnit unit = new DatasetTypeUnit(tableFormat, fileFormat);
            
            unit.setInternalSource(internalSource);
            delegate.run(reader, unit, headerReader.comments(), dataset);

        } catch (Exception e) {
            e.printStackTrace();
            throw new ImporterException("could not import File - " + source + " into Dataset - "
                    + dataset.getName() + "\n" + e.getMessage());
        }

    }

}
