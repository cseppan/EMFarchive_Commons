package gov.epa.emissions.commons.io.exporter.orl;

import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetType;
import gov.epa.emissions.commons.io.ORLDataset;
import gov.epa.emissions.commons.io.Table;
import gov.epa.emissions.commons.io.importer.CommonsTestCase;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ORLDatasetTypes;
import gov.epa.emissions.commons.io.importer.ORLTableTypes;
import gov.epa.emissions.commons.io.importer.TableType;
import gov.epa.emissions.commons.io.importer.orl.BaseORLImporter;

import java.io.File;

public class ORLExporterTest extends CommonsTestCase {

    public void testPoint() throws Exception {
        doImport("ptinv.nti99_NC.txt", ORLDatasetTypes.POINT, ORLTableTypes.ORL_POINT_TOXICS);

        DatasetType datasetType = ORLDatasetTypes.POINT;
        String tableName = "ptinv_nti99_NC";
        File file = createFile(datasetType, tableName);

        doExport(datasetType, ORLTableTypes.ORL_POINT_TOXICS, tableName, file);
    }

    public void testExportSucceedsUsingDefaultSettingsEvenIfFileExists() throws Exception {
        doImport("ptinv.nti99_NC.txt", ORLDatasetTypes.POINT, ORLTableTypes.ORL_POINT_TOXICS);

        DatasetType datasetType = ORLDatasetTypes.POINT;
        String tableName = "ptinv_nti99_NC";
        File file = createFile(datasetType, tableName);
        file.delete();
        assertTrue(file.createNewFile());

        doExport(datasetType, ORLTableTypes.ORL_POINT_TOXICS, tableName, file);
    }

    public void testExportSucceedsWhenOverwriteIsDisabledAndOutputFileDoesNotExist() throws Exception {
        DatasetType datasetType = ORLDatasetTypes.POINT;
        String tableName = "ptinv_nti99_NC";
        File file = createFile(datasetType, tableName);

        doImport("ptinv.nti99_NC.txt", datasetType, ORLTableTypes.ORL_POINT_TOXICS);

        file.delete();
        doExportWithoutOverwrite(datasetType, ORLTableTypes.ORL_POINT_TOXICS, tableName, file);
    }

    public void testExportFailsWhenOverwriteIsDisabledAndOutputFileExists() throws Exception {
        DatasetType datasetType = ORLDatasetTypes.POINT;
        String tableName = "ptinv_nti99_NC";
        File file = createFile(datasetType, tableName);

        doImport("ptinv.nti99_NC.txt", datasetType, ORLTableTypes.ORL_POINT_TOXICS);
        doExport(datasetType, ORLTableTypes.ORL_POINT_TOXICS, tableName, file);

        try {
            doExportWithoutOverwrite(datasetType, ORLTableTypes.ORL_POINT_TOXICS, tableName, file);
        } catch (Exception e) {
            return;
        }

        fail("should have failed to export since file has already been exported and exists");
    }

    public void testNonPoint() throws Exception {
        doImport("arinv.nonpoint.nti99_NC.txt", ORLDatasetTypes.NONPOINT, ORLTableTypes.ORL_AREA_NONPOINT_TOXICS);

        DatasetType datasetType = ORLDatasetTypes.NONPOINT;
        String tableName = "arinv_nonpoint_nti99_NC";
        File file = createFile(datasetType, tableName);

        doExport(datasetType, ORLTableTypes.ORL_AREA_NONPOINT_TOXICS, tableName, file);
    }

    public void testOnRoadMobile() throws Exception {
        doImport("nti99.NC.onroad.SMOKE.txt", ORLDatasetTypes.ON_ROAD, ORLTableTypes.ORL_ONROAD_MOBILE_TOXICS);

        DatasetType datasetType = ORLDatasetTypes.ON_ROAD;
        String tableName = "nti99_NC_onroad_SMOKE";
        File file = createFile(datasetType, tableName);

        doExport(datasetType, ORLTableTypes.ORL_ONROAD_MOBILE_TOXICS, tableName, file);
    }

    public void testNonRoad() throws Exception {
        doImport("arinv.nonroad.nti99d_NC.new.txt", ORLDatasetTypes.NONROAD,
                ORLTableTypes.ORL_AREA_NONROAD_TOXICS);

        DatasetType datasetType = ORLDatasetTypes.NONROAD;
        String tableName = "arinv_nonroad_nti99d_NC_new";
        File file = createFile(datasetType, tableName);

        doExport(datasetType, ORLTableTypes.ORL_AREA_NONROAD_TOXICS, tableName, file);
    }

    private void doExportWithoutOverwrite(DatasetType datasetType, TableType tableType, String tableName, File file)
            throws Exception {
        ORLExporter exporter = ORLExporter.createWithoutOverwrite(dbSetup.getDbServer());
        Dataset dataset = createDataset(datasetType, tableType, tableName);

        exporter.run(dataset, file);
    }

    private void doExport(DatasetType datasetType, TableType tableType, String tableName, File file) throws Exception {
        ORLExporter exporter = ORLExporter.create(dbSetup.getDbServer());
        Dataset dataset = createDataset(datasetType, tableType, tableName);

        exporter.run(dataset, file);
    }

    private File createFile(DatasetType datasetType, String tableName) {
        String tempDir = System.getProperty("java.io.tmpdir");
        String exportFileName = tempDir + "/" + datasetType.getName() + "." + tableName + ".EXPORTED_";

        File file = new File(exportFileName);
        file.deleteOnExit();

        return file;
    }

    private Dataset createDataset(DatasetType datasetType, TableType tableType, String tableName) {
        Dataset dataset = new ORLDataset();
        dataset.setDatasetType(datasetType.getName());
        // only one base type
        dataset.addTable(new Table(tableName, tableType.baseTypes()[0]));
        dataset.setRegion("US");
        dataset.setCountry("US");
        dataset.setYear(1234);
        dataset.setDescription("This is the first line of an artificial description\nThis is the second line");

        return dataset;
    }

    private void doImport(final String filename, DatasetType type, TableType tableType) throws Exception {
        String tableName = filename.substring(0, filename.length() - 4).replace('.', '_');

        Dataset dataset = new ORLDataset();
        dataset.setDatasetType(type.getName());
        // only one base type
        dataset.addTable(new Table(tableName, tableType.baseTypes()[0]));
        dataset.addTable(new Table(tableName + "_summary", tableType.summaryType()));

        Importer importer = new BaseORLImporter(dbSetup.getDbServer(), false, true);
        importer.run(new File[] { new File("test/data/orl/nc", filename) }, dataset, true);
    }
}
