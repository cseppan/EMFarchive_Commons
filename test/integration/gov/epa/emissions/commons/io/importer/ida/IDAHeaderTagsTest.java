package gov.epa.emissions.commons.io.importer.ida;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.DbTestCase;
import gov.epa.emissions.commons.io.importer.temporal.FixedColsTableFormat;
import gov.epa.emissions.framework.db.DbUpdate;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Random;

public class IDAHeaderTagsTest extends DbTestCase {

	private SqlDataTypes sqlDataTypes;

	private Datasource datasource;

	private SimpleDataset dataset;

	protected void setUp() throws Exception {
		super.setUp();
		DbServer dbServer = dbSetup.getDbServer();
		sqlDataTypes = dbServer.getDataType();
		datasource = dbServer.getEmissionsDatasource();

		dataset = new SimpleDataset();
		dataset.setName("test");
		dataset.setDatasetid(new Random().nextLong());
	}

	protected void tearDown() throws Exception {
		DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
		dbUpdate.dropTable(datasource.getName(), dataset.getName());
	}

	public void testShouldIdentifyAllRequiredTags() throws Exception {
		File file = new File("test/data/ida/small-area.txt");
		BufferedReader reader = new BufferedReader(new FileReader(file));
		IDAHeaderReader headerReader = new IDAHeaderReader(reader);
		headerReader.read();

		DatasetTypeUnit unit = createUnit(headerReader);
        
		IDAImporter importer = new IDAImporter(datasource);
		importer.run(reader, unit, headerReader.comments(), dataset);
	}

    private DatasetTypeUnit createUnit(IDAHeaderReader headerReader) {
        FileFormat fileFormat = new IDAAreaFileFormat(headerReader
				.polluntants(), sqlDataTypes);
        FixedColsTableFormat tableFormat = new FixedColsTableFormat(fileFormat, sqlDataTypes);
        DatasetTypeUnit unit = new DatasetTypeUnit(tableFormat, fileFormat);
        return unit;
    }

	public void testShouldIdentifyNoIDATag() throws Exception {
		File file = new File("test/data/ida/noIDATags.txt");
		BufferedReader reader = new BufferedReader(new FileReader(file));
		IDAHeaderReader headerReader = new IDAHeaderReader(reader);
		headerReader.read();
        DatasetTypeUnit unit = createUnit(headerReader);
		IDAImporter importer = new IDAImporter(datasource);
		try {
			importer
					.run(reader, unit, headerReader.comments(), dataset);
			assertTrue(false);
		} catch (Exception e) {
			assertEquals("Could not find tag '#IDA'",e.getMessage());
		}
	}

}
