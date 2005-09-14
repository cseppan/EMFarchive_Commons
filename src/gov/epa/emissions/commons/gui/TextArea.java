package gov.epa.emissions.commons.gui;

import javax.swing.JTextArea;

public class TextArea extends JTextArea {

    public TextArea(String name, String value) {
        super.setName(name);
        super.setText(value);
        super.setRows(2);
        super.setLineWrap(true);
        super.setCaretPosition(0);
        super.setColumns(40);
    }

}
