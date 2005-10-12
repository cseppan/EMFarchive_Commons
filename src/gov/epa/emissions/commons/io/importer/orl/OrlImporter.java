package gov.epa.emissions.commons.io.importer.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;
import gov.epa.emissions.commons.io.importer.DataLoader;
import gov.epa.emissions.commons.io.importer.DelimitedFileReader;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.temporal.TableColumnsMetadata;

import java.io.File;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;

public class OrlImporter {

    private Datasource datasource;

    private TableColumnsMetadata colsMetadata;

    public OrlImporter(Datasource datasource, ColumnsMetadata cols, SqlDataTypes sqlDataTypes) {
        this.datasource = datasource;
        colsMetadata = new TableColumnsMetadata(cols, sqlDataTypes);
    }

    public void run(File file, Dataset dataset) throws ImporterException {
        String table = table(dataset.getName());

        try {
            createTable(table, datasource, colsMetadata);
        } catch (SQLException e) {
            throw new ImporterException("could not create table for dataset - " + dataset.getName(), e);
        }

        try {
            doImport(file, dataset, table, colsMetadata);
        } catch (Exception e) {
            dropTable(table, datasource);
            throw new ImporterException("could not import File - " + file.getAbsolutePath() + " into Dataset - "
                    + dataset.getName());
        }
    }

    private void dropTable(String table, Datasource datasource) throws ImporterException {
        try {
            TableDefinition def = datasource.tableDefinition();
            def.deleteTable(datasource.getName(), table);
        } catch (SQLException e) {
            throw new ImporterException(
                    "could not drop table " + table + " after encountering error importing dataset", e);
        }
    }

    private void doImport(File file, Dataset dataset, String table, ColumnsMetadata colsMetadata) throws Exception {
        DataLoader loader = new DataLoader(datasource, colsMetadata);
        Reader reader = new DelimitedFileReader(file);

        loader.load(reader, dataset, table);
        loadDataset(reader.comments(), dataset);
    }

    private void loadDataset(List comments, Dataset dataset) {
        StringBuffer description = new StringBuffer();

        for (Iterator iter = comments.iterator(); iter.hasNext();) {
            String comment = (String) iter.next();
            if (comment.startsWith("#COUNTRY")) {
                String country = comment.substring("#COUNTRY".length()).trim();
                dataset.setCountry(country);
                dataset.setRegion(country);
            }

            if (comment.startsWith("#YEAR")) {
                String year = comment.substring("#YEAR".length()).trim();
                int yearInt = Integer.parseInt(year);
                dataset.setYear(yearInt);

                setStartStopDateTimes(dataset, yearInt);
            }

            // TODO: this probably applies to all importers
            if (comment.startsWith("#DESC"))
                description.append(comment.substring("#DESC".length()) + "\n");
        }

        dataset.setDescription(description.toString());
    }

    private void setStartStopDateTimes(Dataset dataset, int year) {
        Date start = new GregorianCalendar(year, Calendar.JANUARY, 1).getTime();
        dataset.setStartDateTime(start);

        Calendar endCal = new GregorianCalendar(year, Calendar.DECEMBER, 31, 23, 59, 59);
        endCal.set(Calendar.MILLISECOND, 999);
        dataset.setStopDateTime(endCal.getTime());
    }

    private String table(String datasetName) {
        return datasetName.trim().replaceAll(" ", "_");
    }

    private void createTable(String table, Datasource datasource, ColumnsMetadata colsMetadata) throws SQLException {
        TableDefinition tableDefinition = datasource.tableDefinition();
        tableDefinition.createTable(datasource.getName(), table, colsMetadata.colNames(), colsMetadata.colTypes());
    }

}
