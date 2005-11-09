package gov.epa.emissions.commons.io.importer.generic;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.temporal.TableFormat;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class LineImporter implements Importer {

    private Datasource datasource;

    private File file;

    private FormatUnit typeUnit;

    public LineImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
        this.datasource = datasource;
        FileFormat fileFormat = new LineFileFormat(sqlDataTypes);
        TableFormat tableFormat = new LineTableFormat(fileFormat, sqlDataTypes);
        typeUnit = new DatasetTypeUnit(tableFormat, fileFormat);
    }

    // TODO: verify if file exists
    public void preCondition(File folder, String filePattern) {
        this.file = new File(folder, filePattern);
    }

    public void run(Dataset dataset) throws ImporterException {
        String table = table(dataset.getName());

        try {
            createTable(table, datasource, typeUnit.tableFormat());
        } catch (SQLException e) {
            throw new ImporterException("could not create table for dataset - " + dataset.getName(), e);
        }

        try {
            doImport(file, dataset, table, typeUnit.tableFormat());
        } catch (Exception e) {
            dropTable(table, datasource);
            throw new ImporterException("could not import File - " + file.getAbsolutePath() + " into Dataset - "
                    + dataset.getName());
        }

    }

    private void dropTable(String table, Datasource datasource) throws ImporterException {
        try {
            TableDefinition def = datasource.tableDefinition();
            def.deleteTable(table);
        } catch (SQLException e) {
            throw new ImporterException(
                    "could not drop table " + table + " after encountering error importing dataset", e);
        }
    }

    private void doImport(File file, Dataset dataset, String table, TableFormat tableFormat) throws Exception {
        LineLoader loader = new LineLoader(datasource, tableFormat);
        Reader reader = new LineReader(file);

        loader.load(reader, dataset, table);
        loadDataset(file, table, tableFormat, dataset);
    }

    private void loadDataset(File file, String table, TableFormat colsMetadata, Dataset dataset) {
        setInternalSource(file, table, colsMetadata, dataset);
    }

    // TODO: this applies to all the Importers. Needs to be pulled out
    private void setInternalSource(File file, String table, TableFormat colsMetadata, Dataset dataset) {
        InternalSource source = new InternalSource();
        source.setTable(table);
        source.setType(colsMetadata.identify());
        source.setCols(colNames(colsMetadata.cols()));
        source.setSource(file.getAbsolutePath());
        source.setSourceSize(file.length());

        dataset.addInternalSource(source);
    }

    private String[] colNames(Column[] cols) {
        List names = new ArrayList();
        for (int i = 0; i < cols.length; i++)
            names.add(cols[i].name());

        return (String[]) names.toArray(new String[0]);
    }

    private String table(String datasetName) {
        return datasetName.trim().replaceAll(" ", "_");
    }

    private void createTable(String table, Datasource datasource, TableFormat tableFormat) throws SQLException {
        TableDefinition tableDefinition = datasource.tableDefinition();
        tableDefinition.createTable(table, tableFormat.cols());
    }
}
