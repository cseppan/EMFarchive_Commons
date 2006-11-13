package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.generic.GenericExporter;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.NonVersionedDataFormatFactory;

public class IDAActivityExporter extends GenericExporter {

    public IDAActivityExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDatatypes, Integer optimizedBatchSize)
            throws ImporterException {
        super(dataset, dbServer, fileFormat(sqlDatatypes, new NonVersionedDataFormatFactory()), optimizedBatchSize);
        setup(dataset);
    }

    public IDAActivityExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes,
            DataFormatFactory dataFormatFactory, Integer optimizedBatchSize) throws ImporterException {
        super(dataset, dbServer, fileFormat(sqlDataTypes, dataFormatFactory), dataFormatFactory, optimizedBatchSize);
        setup(dataset);
    }

    private void setup(Dataset dataset) throws ImporterException {
        setDelimiter(" ");
        String[] comments = comments(dataset.getDescription());
        ((IDAFileFormat) fileFormat).addPollutantCols(pollutantCols(comments));
    }

    private String[] comments(String description) {
        return description.split("\n");
    }

    private String[] pollutantCols(String[] comments) throws ImporterException {
        IDAPollutantParser parser = new IDAPollutantParser();
        parser.processComments(comments);
        String[] pollutants = parser.pollutants();
        return pollutants;
    }

    private static IDAFileFormat fileFormat(SqlDataTypes sqlDatatypes, DataFormatFactory factory) {
        return new IDAActivityFileFormat(sqlDatatypes, factory.defaultValuesFiller());
    }

}
