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

public class IDAMobileImporter {

    private IDAImporter delegate;

    private SqlDataTypes sqlDataTypes;

    public IDAMobileImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
        this.sqlDataTypes = sqlDataTypes;
        delegate = new IDAImporter(datasource);
    }

    public void run(Dataset dataset) throws ImporterException {
        InternalSource internalSource = dataset.getInternalSources()[0];
        String source = internalSource.getSource();
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(source));
            IDAHeaderReader headerReader = new IDAHeaderReader(reader);
            headerReader.read();

            FileFormat fileFormat = new IDAMobileFileFormat(headerReader.polluntants(), sqlDataTypes);
            FixedColsTableFormat tableFormat = new FixedColsTableFormat(fileFormat, sqlDataTypes);
            DatasetTypeUnit unit = new DatasetTypeUnit(tableFormat, fileFormat);
            unit.setInternalSource(internalSource);
            
            delegate.run(reader, unit, headerReader.comments(), dataset);

        } catch (Exception e) {
            throw new ImporterException("could not import File - " + source + " into Dataset - "
                    + dataset.getName());
        }

    }

}
