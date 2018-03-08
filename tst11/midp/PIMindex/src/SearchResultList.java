import javax.microedition.lcdui.*;
import org.garret.perst.fulltext.*;

public class SearchResultList extends List implements CommandListener  
{
    PIMindex main;
    FullTextSearchResult result;
    
    public SearchResultList(PIMindex main, String query, FullTextSearchResult result)
    {
        super("Found " + result.estimation + " results", IMPLICIT);
        this.main = main;
        this.result = result;

        for (int i = 0; i < result.hits.length; i++) { 
            ContactDetails cd = (ContactDetails)result.hits[i].getDocument();
            append(cd.buildSnippet(query, main.index.getHelper()), null);
        }
        setCommandListener(this);
        addCommand(PIMindex.BACK_CMD);
        Display.getDisplay(main).setCurrent(this);
    }
    
    public void commandAction(Command c, Displayable d)  
    {
        if (c == PIMindex.BACK_CMD) {
            Display.getDisplay(main).setCurrent(main.searchForm);
        } else { 
            int i = getSelectedIndex();
            if (i >= 0) { 
                new SearchResultForm(main, result.hits[i], this);
            }
        }
    }
}