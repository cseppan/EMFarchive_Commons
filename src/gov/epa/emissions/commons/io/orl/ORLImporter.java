package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbColumn;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.importer.DelimiterIdentifyingFileReader;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.HelpImporter;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.OptionalColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.TableFormatWithOptionalCols;
import gov.epa.emissions.commons.io.importer.TemporalResolution;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;

public class ORLImporter {

    private Dataset dataset;

    private Datasource datasource;

    private File file;

    private FormatUnit formatUnit;

    private HelpImporter delegate;

    private SqlDataTypes sqlDataTypes;

    public ORLImporter(Dataset dataset, FormatUnit formatUnit, Datasource datasource, SqlDataTypes sqlDataTypes) {
        this.dataset = dataset;
        this.formatUnit = formatUnit;
        this.datasource = datasource;
        this.delegate = new HelpImporter();
        this.sqlDataTypes = sqlDataTypes;
    }

    public void setup(File file) throws ImporterException {
        validateORLFile(file);
        this.file = file;
    }

    public void run() throws ImporterException {
        String table = delegate.tableName(dataset.getName());
        delegate.createTable(table, datasource, formatUnit.tableFormat(), dataset.getName());
        try {
            doImport(file, dataset, table, (FileFormatWithOptionalCols) formatUnit.fileFormat(),
                    (TableFormatWithOptionalCols) formatUnit.tableFormat());
        } catch (Exception e) {
            delegate.dropTable(table, datasource);
            throw new ImporterException("Filename: " + file.getAbsolutePath() + ", " + e.getMessage());
        }
    }

    private void doImport(File file, Dataset dataset, String table, FileFormatWithOptionalCols fileFormat,
            TableFormatWithOptionalCols tableFormat) throws Exception {
        OptionalColumnsDataLoader loader = new OptionalColumnsDataLoader(datasource, tableFormat);
        Reader reader = new DelimiterIdentifyingFileReader(file, fileFormat.minCols().length);
        loader.load(reader, dataset, table);

        addVersionZeroEntryToVersionsTable(datasource, dataset);
        loadDataset(file, table, tableFormat, reader.comments(), dataset);
    }

    private void addVersionZeroEntryToVersionsTable(Datasource datasource, Dataset dataset) throws SQLException {
        DataModifier modifier = datasource.dataModifier();
        String[] data = { dataset.getDatasetid() + "", "0", "", "true" };
        DbColumn[] cols = { new Column("dataset_id", sqlDataTypes.longType()),
                new Column("version", sqlDataTypes.intType()), new Column("path", sqlDataTypes.text()),
                new Column("final_version", sqlDataTypes.booleanType()) };

        // TODO: where should the 'versions' table name be defined?
        modifier.insertRow("versions", data, cols);
    }

    private void loadDataset(File file, String table, FileFormat fileFormat, List comments, Dataset dataset) {
        delegate.setInternalSource(file, table, fileFormat, dataset);
        dataset.setUnits("short tons/year");
        dataset.setTemporalResolution(TemporalResolution.ANNUAL.getName());
        dataset.setDescription(delegate.descriptions(comments));
    }

    private void validateORLFile(File file) throws ImporterException {
        delegate.validateFile(file);
        Reader reader = null;
        try {
            reader = new DelimiterIdentifyingFileReader(file, ((FileFormatWithOptionalCols) formatUnit.fileFormat())
                    .minCols().length);
            reader.read();
            reader.close();
        } catch (IOException e) {
            throw new ImporterException(e.getMessage());
        }
        List comments = reader.comments();
        addAttributesExtractedFromComments(comments, dataset);
    }

    private void addAttributesExtractedFromComments(List comments, Dataset dataset) throws ImporterException {
        if (!(tag("#ORL", comments) != null))
            throw new ImporterException("The tag - 'ORL' is mandatory.");

        String country = tag("#COUNTRY", comments);
        if (country == null || country.length() == 0)
            throw new ImporterException("The tag - 'COUNTRY' is mandatory.");
        dataset.setCountry(country);
        dataset.setRegion(country);

        String year = tag("#YEAR", comments);
        if (year == null || year.length() == 0)
            throw new ImporterException("The tag - 'YEAR' is mandatory.");
        dataset.setYear(Integer.parseInt(year));
        setStartStopDateTimes(dataset, Integer.parseInt(year));

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

}
