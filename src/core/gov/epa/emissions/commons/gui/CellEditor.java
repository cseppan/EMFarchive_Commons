package gov.epa.emissions.commons.gui;

import gov.epa.emissions.commons.io.ColumnMetaData;

import java.awt.Component;

import javax.swing.AbstractCellEditor;
import javax.swing.JTable;
import javax.swing.table.TableCellEditor;

public class CellEditor extends AbstractCellEditor implements TableCellEditor {

    private TextField textField;

    private Object value;

    private Object editedValue;

    private ColumnMetaData column;

    public CellEditor(ColumnMetaData column) {
        this.column = column;
        textField = new TextField(column.getName(), column.getSize());
    }

    public Object getCellEditorValue() {
        return value;
    }

    public Component getTableCellEditorComponent(JTable table, Object value, boolean isSelected, int row, int column) {
        this.editedValue = value;
        this.textField.setText("" + value);
        return textField;
    }

    public boolean stopCellEditing() {
        if (validate(editedValue)) {
            this.value = editedValue;
        }
        return super.stopCellEditing();
    }

    private boolean validate(Object editedValue) {
        // TODO: check for null values
        String value = editedValue.toString();
        if (sizeCheck(value, column.getSize())) {
            return false;
        }

        Class columnClass = classType(column.getType());
        if (columnClass == Double.class || columnClass == Float.class) {
            return doubleValue(value);
        } else if (columnClass == Integer.class) {
            return integerValue(value);
        }

        this.value = editedValue;
        return true;
    }

    private Class classType(String type) {
        try {
            return Class.forName(type);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e.getMessage());//TODO: what to do with the exception
        }
    }

    private boolean sizeCheck(String value, int size) {
        if (size != -1 && value.length() <= size) {
            return true;
        }
        // TODO: message
        return false;
    }

    private boolean integerValue(String editedValue) {
        try {
            this.value = Integer.valueOf(editedValue.toString());
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private boolean doubleValue(String editedValue) {
        try {
            this.value = Double.valueOf(editedValue.toString());
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
