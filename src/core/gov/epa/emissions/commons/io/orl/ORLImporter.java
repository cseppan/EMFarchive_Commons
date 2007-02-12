package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.data.DatasetTypeUnit;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.TableFormat;
import gov.epa.emissions.commons.io.importer.Comments;
import gov.epa.emissions.commons.io.importer.DataTable;
import gov.epa.emissions.commons.io.importer.DatasetLoader;
import gov.epa.emissions.commons.io.importer.DelimiterIdentifyingFileReader;
import gov.epa.emissions.commons.io.importer.FileVerifier;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.OptionalColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.TemporalResolution;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

public class ORLImporter {

    private Dataset dataset;

    private Datasource datasource;

    private File file;

    private FormatUnit formatUnit;

    private DataTable dataTable;

    private DatasetLoader loader;

    public ORLImporter(File folder, String[] filePatterns, Dataset dataset, DatasetTypeUnit formatUnit,
            Datasource datasource) throws ImporterException {
        new FileVerifier().shouldHaveOneFile(filePatterns);
        this.file = new File(folder, filePatterns[0]);

        this.dataset = dataset;
        this.formatUnit = formatUnit;
        this.datasource = datasource;
        dataTable = new DataTable(dataset, datasource);
        loader = new DatasetLoader(dataset);
        loader.internalSource(file, dataTable.name(), formatUnit.tableFormat());
    }

    public void run() throws ImporterException {
        importAttributes(file, dataset);
        dataTable.create(formatUnit.tableFormat());
        try {
            doImport(file, dataset, dataTable.name(), (FileFormatWithOptionalCols) formatUnit.fileFormat(), formatUnit
                    .tableFormat());
        } catch (Exception e) {
            e.printStackTrace();
            dataTable.drop();
            throw new ImporterException("Filename: " + file.getAbsolutePath() + ", " + e.getMessage());
        }
    }

    private void doImport(File file, Dataset dataset, String table, FileFormatWithOptionalCols fileFormat,
            TableFormat tableFormat) throws Exception {
        Reader reader = null;
        try {
            OptionalColumnsDataLoader loader = new OptionalColumnsDataLoader(datasource, fileFormat, tableFormat.key());
            reader = new DelimiterIdentifyingFileReader(file, fileFormat.minCols().length);
            loader.load(reader, dataset, table);

            loadDataset(reader.comments(), dataset);
        } finally {
            close(reader);
        }
    }

    private void close(Reader reader) throws ImporterException {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                throw new ImporterException(e.getMessage());
            }
        }
    }

    private void loadDataset(List comments, Dataset dataset) {
        dataset.setUnits("short tons/year");
        dataset.setTemporalResolution(TemporalResolution.ANNUAL.getName());
        dataset.setDescription(new Comments(comments).all());
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
        addAttributes(reader.comments(), dataset);
    }

    private void addAttributes(List commentsList, Dataset dataset) throws ImporterException {
        Comments comments = new Comments(commentsList);
        if (!comments.have("ORL"))
            throw new ImporterException("The tag - 'ORL' is mandatory.");

        if (!comments.hasContent("COUNTRY"))
            throw new ImporterException("The tag - 'COUNTRY' is mandatory.");
        //BUG: Country should not be created, but looked up
        // String country = comments.content("COUNTRY");
        // dataset.setCountry(new Country(country));

        if (!comments.hasContent("YEAR"))
            throw new ImporterException("The tag - 'YEAR' is mandatory.");
        String year = comments.content("YEAR");
        dataset.setYear(Integer.parseInt(year));
        setStartStopDateTimes(dataset, Integer.parseInt(year));
    }

    private void setStartStopDateTimes(Dataset dataset, int year) {
        Date start = new GregorianCalendar(year, Calendar.JANUARY, 1).getTime();
        dataset.setStartDateTime(start);

        Calendar endCal = new GregorianCalendar(year, Calendar.DECEMBER, 31, 23, 59, 59);
        endCal.set(Calendar.MILLISECOND, 999);
        dataset.setStopDateTime(endCal.getTime());
    }

}
