package gov.epa.emissions.commons.io.speciation;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.FixedColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.HelpImporter;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.WhitespaceDelimitedTokenizer;
import gov.epa.emissions.commons.io.temporal.FixedColsTableFormat;
import gov.epa.emissions.commons.io.temporal.TableFormat;

import java.io.File;
import java.util.List;

public class SpeciationCrossReferenceImporter {
    private Datasource datasource;

    private File file;

    private FormatUnit formatUnit;

    private HelpImporter delegate;

    public SpeciationCrossReferenceImporter(Datasource datasource, SqlDataTypes sqlDataTypes, String identifier) {
        this.datasource = datasource;
        FileFormat fileFormat = new SpeciationCrossRefFileFormat(identifier, sqlDataTypes);
        TableFormat tableFormat = new FixedColsTableFormat(fileFormat, sqlDataTypes);
        formatUnit = new DatasetTypeUnit(tableFormat, fileFormat);
        this.delegate = new HelpImporter();
    }

    // TODO: verify if file exists
    public void preCondition(File folder, String filePattern) {
        File file = new File(folder, filePattern);
        try {
            delegate.validateFile(file);
        } catch (ImporterException e) {
            e.printStackTrace();
        }
        
        this.file = file;
    }

    public void run(Dataset dataset) throws ImporterException {
        String table = delegate.tableName(dataset.getName());

        delegate.createTable(table,datasource,formatUnit.tableFormat(),dataset.getName());
        try {
            doImport(file, dataset, table, formatUnit.tableFormat());
        } catch (Exception e) {
            delegate.dropTable(table, datasource);
            throw new ImporterException("could not import File - " + file.getAbsolutePath() + " into Dataset - "
                    + dataset.getName());
        }
    }

    // FIXME: have to use a delimited identifying reader
    private void doImport(File file, Dataset dataset, String table, TableFormat tableFormat) throws Exception {
        FixedColumnsDataLoader loader = new FixedColumnsDataLoader(datasource, tableFormat);
        Reader reader = new SpeciationCrossReferenceReader(file, formatUnit.fileFormat(), new WhitespaceDelimitedTokenizer());

        loader.load(reader, dataset, table);
        loadDataset(file, table, formatUnit.fileFormat(), dataset, reader.comments());
    }

    private void loadDataset(File file, String table, FileFormat fileFormat, Dataset dataset, List comments) {
        delegate.setInternalSource(file, table, fileFormat, dataset);
        dataset.setDescription(delegate.descriptions(comments));
    }
    
}
