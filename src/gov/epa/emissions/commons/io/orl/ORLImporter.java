package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;
import gov.epa.emissions.commons.io.importer.DataLoader;
import gov.epa.emissions.commons.io.importer.DelimitedFileReader;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.TemporalResolution;
import gov.epa.emissions.commons.io.importer.temporal.TableColumnsMetadata;

import java.io.File;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;

public class ORLImporter {

    private Datasource datasource;

    private TableColumnsMetadata colsMetadata;

    public ORLImporter(Datasource datasource, ColumnsMetadata cols, SqlDataTypes sqlDataTypes) {
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
            def.deleteTable(table);
        } catch (SQLException e) {
            throw new ImporterException(
                    "could not drop table " + table + " after encountering error importing dataset", e);
        }
    }

    private void doImport(File file, Dataset dataset, String table, TableColumnsMetadata colsMetadata) throws Exception {
        DataLoader loader = new DataLoader(datasource, colsMetadata);
        Reader reader = new DelimitedFileReader(file);

        loader.load(reader, dataset, table);
        loadDataset(file, table, colsMetadata, reader.comments(), dataset);
    }

    private void loadDataset(File file, String table, TableColumnsMetadata colsMetadata, List comments, Dataset dataset)
            throws ImporterException {
        setInternalSource(file, table, colsMetadata, dataset);
        addAttributesExtractedFromComments(comments, dataset);
        dataset.setUnits("short tons/year");
        dataset.setTemporalResolution(TemporalResolution.ANNUAL.getName());
    }

    // TODO: this applies to all the Importers. Needs to be pulled out
    private void setInternalSource(File file, String table, TableColumnsMetadata colsMetadata, Dataset dataset) {
        InternalSource source = new InternalSource();
        source.setTable(table);
        source.setType(colsMetadata.identify());
        source.setCols(colsMetadata.colNames());
        source.setSource(file.getAbsolutePath());
        source.setSourceSize(file.length());

        dataset.addInternalSource(source);
    }

    private void addAttributesExtractedFromComments(List comments, Dataset dataset) throws ImporterException {
        // TODO: this probably applies to all importers
        if (!(tag("#ORL", comments) != null))
            throw new ImporterException("The tag - 'ORL' is mandatory, and is invalid");

        String country = tag("#COUNTRY", comments);
        if (country == null || country.length() == 0)
            throw new ImporterException("The tag - 'COUNTRY' is mandatory, and is invalid");
        dataset.setCountry(country);
        dataset.setRegion(country);

        String year = tag("#YEAR", comments);
        if (year == null || year.length() == 0)
            throw new ImporterException("The tag - 'YEAR' is mandatory, and is invalid");
        dataset.setYear(Integer.parseInt(year));
        setStartStopDateTimes(dataset, Integer.parseInt(year));

        dataset.setDescription(descriptions(comments));
    }

    private String descriptions(List comments) {
        StringBuffer description = new StringBuffer();
        for (Iterator iter = comments.iterator(); iter.hasNext();)
            description.append(iter.next() + "\n");

        return description.toString();
    }

    private String tag(String tag, List comments) {
        for (Iterator iter = comments.iterator(); iter.hasNext();) {
            String comment = (String) iter.next();
            if (comment.startsWith(tag)) {
                return comment.substring(tag.length()).trim();
            }
        }

        return null;
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
        tableDefinition.createTable(table, colsMetadata.colNames(), colsMetadata.colTypes());
    }

}
