package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnitWithOptionalCols;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.importer.DelimiterIdentifyingFileReader;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.OptionalColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.TableFormatWithOptionalCols;
import gov.epa.emissions.commons.io.importer.TemporalResolution;
import gov.epa.emissions.commons.io.importer.temporal.TableFormat;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;

public class ORLImporter implements Importer {

    private Datasource datasource;

    private File file;

    private DatasetTypeUnitWithOptionalCols unit;

    public ORLImporter(Datasource datasource, DatasetTypeUnitWithOptionalCols unit) {
        this.datasource = datasource;
        this.unit = unit;
    }

    // TODO: verify if file exists
    public void preCondition(File folder, String filePattern) {
        this.file = new File(folder, filePattern);
    }

    public void run(Dataset dataset) throws ImporterException {
        String table = table(dataset.getName());

        try {
            createTable(table, datasource, unit.tableFormat());
        } catch (SQLException e) {
            throw new ImporterException("could not create table for dataset - " + dataset.getName(), e);
        }

        try {
            doImport(file, dataset, table, unit.fileFormat(), unit.tableFormat());
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

    private void doImport(File file, Dataset dataset, String table, FileFormatWithOptionalCols fileFormat,
            TableFormatWithOptionalCols tableFormat) throws Exception {
        OptionalColumnsDataLoader loader = new OptionalColumnsDataLoader(datasource, tableFormat);
        Reader reader = new DelimiterIdentifyingFileReader(file, fileFormat.minCols().length);

        loader.load(reader, dataset, table);
        loadDataset(file, table, tableFormat, reader.comments(), dataset);
    }

    private void loadDataset(File file, String table, FileFormat colsMetadata, List comments, Dataset dataset)
            throws ImporterException {
        setInternalSource(file, table, colsMetadata, dataset);
        addAttributesExtractedFromComments(comments, dataset);
        dataset.setUnits("short tons/year");
        dataset.setTemporalResolution(TemporalResolution.ANNUAL.getName());
    }

    // TODO: this applies to all the Importers. Needs to be pulled out
    private void setInternalSource(File file, String table, FileFormat colsMetadata, Dataset dataset) {
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

    private void addAttributesExtractedFromComments(List comments, Dataset dataset) throws ImporterException {
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

        // TODO: this probably applies to all importers
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

    private void createTable(String table, Datasource datasource, TableFormat tableFormat) throws SQLException {
        TableDefinition tableDefinition = datasource.tableDefinition();
        tableDefinition.createTable(table, tableFormat.cols());
    }

}
