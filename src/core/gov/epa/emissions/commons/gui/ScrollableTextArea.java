package gov.epa.emissions.commons.gui;

import javax.swing.JScrollPane;

public class ScrollableTextArea extends JScrollPane {

    public ScrollableTextArea(TextArea text) {
        text.setLineWrap(false);

        super.setViewportView(text);
        super.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED);
        super.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
    }

    public static ScrollableTextArea createWithVerticalScrollBar(TextArea text) {
        ScrollableTextArea scrollable = new ScrollableTextArea(text);
        scrollable.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_NEVER);
        
        return scrollable;
    }

}
