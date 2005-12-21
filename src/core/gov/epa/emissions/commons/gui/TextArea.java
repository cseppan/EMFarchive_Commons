package gov.epa.emissions.commons.gui;

import javax.swing.JTextArea;

public class TextArea extends JTextArea {

    public TextArea(String name, String value) {
        this(name, value, 40);
    }
    
    public TextArea(String name, String value, int width) {
        super.setName(name);
        super.setText(value);
        super.setRows(4);
        super.setLineWrap(true);
        super.setCaretPosition(0);
        super.setColumns(width);
    }

}
