package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;

import org.jmock.Mock;
import org.jmock.MockObjectTestCase;

public class VersionedTableFormatWithOptionalColsTest extends MockObjectTestCase {

    public void testRecordIdShouldBeTheKey() {
        Mock types = mock(SqlDataTypes.class);
        types.stubs().method(ANYTHING).will(returnValue(""));

        Mock fileFormat = mock(FileFormatWithOptionalCols.class);
        fileFormat.stubs().method("cols").will(returnValue(new Column[0]));

        VersionedTableFormatWithOptionalCols format = new VersionedTableFormatWithOptionalCols(
                (FileFormatWithOptionalCols) fileFormat.proxy(), (SqlDataTypes) types.proxy());

        assertEquals("Record_Id", format.key());
    }

    public void testShouldHaveFourFixedVersionRelatedColumns() {
        Mock types = mock(SqlDataTypes.class);
        types.stubs().method("longType").will(returnValue("long"));
        types.stubs().method("stringType").withAnyArguments().will(returnValue("long"));
        types.stubs().method("autoIncrement").withAnyArguments().will(returnValue("serial"));
        types.stubs().method("text").withAnyArguments().will(returnValue("text"));

        Mock fileFormat = mock(FileFormatWithOptionalCols.class);
        fileFormat.stubs().method("cols").will(returnValue(new Column[0]));

        VersionedTableFormatWithOptionalCols format = new VersionedTableFormatWithOptionalCols(
                (FileFormatWithOptionalCols) fileFormat.proxy(), (SqlDataTypes) types.proxy());

        Column[] cols = format.cols();

        assertEquals(5, cols.length);
        // version-related cols
        assertEquals("Record_Id", cols[0].name());
        assertEquals("Dataset_Id", cols[1].name());
        assertEquals("Version", cols[2].name());
        assertEquals("Delete_Versions", cols[3].name());

        assertEquals("Comments", cols[4].name());
    }

    public void testShouldPlaceDataColsBetweenVersionColumnsAndCommentsCols() {
        Mock types = mock(SqlDataTypes.class);
        types.stubs().method(ANYTHING).will(returnValue(""));

        Mock fileFormat = mock(FileFormatWithOptionalCols.class);
        Column dataCol = new Column("data", "type");
        fileFormat.stubs().method("cols").will(returnValue(new Column[] { dataCol }));

        VersionedTableFormatWithOptionalCols format = new VersionedTableFormatWithOptionalCols(
                (FileFormatWithOptionalCols) fileFormat.proxy(), (SqlDataTypes) types.proxy());

        Column[] cols = format.cols();

        assertEquals(6, cols.length);

        // version-related cols
        assertEquals("Record_Id", cols[0].name());
        assertEquals("Dataset_Id", cols[1].name());
        assertEquals("Version", cols[2].name());
        assertEquals("Delete_Versions", cols[3].name());

        assertSame(dataCol, cols[4]);

        assertEquals("Comments", cols[5].name());
    }

}
