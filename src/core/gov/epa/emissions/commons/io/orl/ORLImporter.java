package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.TableFormat;
import gov.epa.emissions.commons.io.importer.DelimiterIdentifyingFileReader;
import gov.epa.emissions.commons.io.importer.HelpImporter;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.OptionalColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.TemporalResolution;

import java.io.File;
import java.io.IOException;
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

    public ORLImporter(File file, Dataset dataset, FormatUnit formatUnit, Datasource datasource) {
        this.dataset = dataset;
        this.formatUnit = formatUnit;
        this.datasource = datasource;
        this.delegate = new HelpImporter();
        this.file = file;
    }

    public void run() throws ImporterException {
        importAttributes(file, dataset);

        String table = delegate.tableName(dataset.getName());
        delegate.createTable(table, datasource, formatUnit.tableFormat());

        try {
            doImport(file, dataset, table, (FileFormatWithOptionalCols) formatUnit.fileFormat(), formatUnit
                    .tableFormat());
        } catch (Exception e) {
            delegate.dropTable(table, datasource);
            throw new ImporterException("Filename: " + file.getAbsolutePath() + ", " + e.getMessage());
        }
    }

    private void doImport(File file, Dataset dataset, String table, FileFormatWithOptionalCols fileFormat,
            TableFormat tableFormat) throws Exception {
        OptionalColumnsDataLoader loader = new OptionalColumnsDataLoader(datasource, fileFormat, tableFormat.key());
        Reader reader = new DelimiterIdentifyingFileReader(file, fileFormat.minCols().length);
        loader.load(reader, dataset, table);

        loadDataset(file, table, tableFormat, reader.comments(), dataset);
    }

    private void loadDataset(File file, String table, TableFormat tableFormat, List comments, Dataset dataset) {
        delegate.setInternalSource(file, table, tableFormat, dataset);
        dataset.setUnits("short tons/year");
        dataset.setTemporalResolution(TemporalResolution.ANNUAL.getName());
        dataset.setDescription(delegate.descriptions(comments));
    }

    private void importAttributes(File file, Dataset dataset) throws ImporterException {
        Reader reader = null;
        try {
            // FIXME: move 'minCols' to FileFormat
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
