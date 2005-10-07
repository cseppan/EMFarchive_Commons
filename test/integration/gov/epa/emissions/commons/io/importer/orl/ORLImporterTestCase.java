package gov.epa.emissions.commons.io.importer.orl;

import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetType;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.Table;
import gov.epa.emissions.commons.io.importer.DbTestCase;
import gov.epa.emissions.commons.io.importer.DefaultORLDatasetTypesFactory;
import gov.epa.emissions.commons.io.importer.ORLDatasetTypesFactory;
import gov.epa.emissions.commons.io.importer.ORLTableType;
import gov.epa.emissions.commons.io.importer.ORLTableTypes;

public abstract class ORLImporterTestCase extends DbTestCase {

    protected ORLDatasetTypesFactory types;

    private ORLTableTypes orlTableTypes;

    protected void setUp() throws Exception {
        super.setUp();

        this.types = new DefaultORLDatasetTypesFactory();
        orlTableTypes = new ORLTableTypes(types);
    }

    abstract protected void doImport(String filename, Dataset dataset) throws Exception;

    public void testNonPoint() throws Exception {
        run("arinv.nonpoint.nti99_NC.txt", types.nonPoint(), orlTableTypes.nonPoint());
    }

    private void run(String filename, DatasetType datasetType, ORLTableType tableType) throws Exception {
        String table = filename.substring(0, filename.length() - 4).replace('.', '_');

        Dataset dataset = new SimpleDataset();
        dataset.setDatasetType(datasetType);
        // only one base type
        dataset.addTable(new Table(table, tableType.base()));
        dataset.addTable(new Table(table + "_summary", tableType.summary()));

        doImport(filename, dataset);
    }

    public void testNonRoad() throws Exception {
        run("arinv.nonroad.nti99d_NC.new.txt", types.nonRoad(), orlTableTypes.nonRoad());
    }

    public void testNonRoadWithFewerColumns() throws Exception {
        run("Nonroad_withFewerColumns.txt", types.nonRoad(), orlTableTypes.nonRoad());
    }

    public void testPoint() throws Exception {
        run("ptinv.nti99_NC.txt", types.point(), orlTableTypes.point());
    }

    public void testPointWithApostrophe() throws Exception {
        run("point_withApostrophe.orl", types.point(), orlTableTypes.point());
    }

    public void testOnRoadMobile() throws Exception {
        run("nti99.NC.onroad.SMOKE.txt", types.onRoad(), orlTableTypes.onRoad());
    }

}
