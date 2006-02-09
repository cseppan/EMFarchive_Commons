package gov.epa.emissions.commons.gui;

import javax.swing.JTextArea;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.text.Document;

public class TextArea extends JTextArea implements Changeable {
    private ChangeablesList listOfChangeables;
    
    private boolean changed = false;
    
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
    
    public TextArea(String name, String value, int width, int rows) {
        super.setName(name);
        super.setText(value);
        super.setRows(rows);
        super.setLineWrap(true);
        super.setCaretPosition(0);
        super.setColumns(width);
    }
    
    public void addTextListener() {
        Document nameDoc = this.getDocument();
        nameDoc.addDocumentListener(new DocumentListener() {
            public void changedUpdate(DocumentEvent e) {
                notifyChanges();
            }

            public void insertUpdate(DocumentEvent e) {
                notifyChanges();
            }

            public void removeUpdate(DocumentEvent e) {
                notifyChanges();
            }
        });
    }
    
    public void clear() {
        this.changed = false;
    }
    
    private void notifyChanges() {
        this.changed = true;
        this.listOfChangeables.onChanges();
    }

    public boolean hasChanges() {
        return this.changed;
    }

    public void observe(ChangeablesList list) {
        this.listOfChangeables = list;
    }

}
