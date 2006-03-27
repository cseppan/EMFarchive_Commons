package gov.epa.emissions.commons.gui;

import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;

import javax.swing.JTextArea;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.text.Document;

public class TextArea extends JTextArea implements Changeable {
    private Changeables changeables;

    private boolean changed = false;

    public TextArea(String name, String value) {
        this(name, value, 40);
    }

    public TextArea(String name, String value, int width) {
        this(name, value, width, 4);
    }

    public TextArea(String name, String value, int width, int rows) {
        super.setName(name);
        super.setText(value);
        super.setRows(rows);
        super.setLineWrap(true);
        super.setCaretPosition(0);
        super.setColumns(width);
        super.setWrapStyleWord(true);
    }

    private void addTextListener() {
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

    private void addKeyListener() {
        this.addKeyListener(new KeyAdapter() {
            public void keyTyped(KeyEvent e) {
                notifyChanges();
            }
        });
    }

    public void clear() {
        this.changed = false;
    }

    void notifyChanges() {
        changed = true;
        if (changeables != null)
            changeables.onChanges();
    }

    public boolean hasChanges() {
        return this.changed;
    }

    public void observe(Changeables changeables) {
        this.changeables = changeables;
        addTextListener();
        addKeyListener();
    }

    public boolean isEmpty() {
        return getText().trim().length() == 0;
    }

}
