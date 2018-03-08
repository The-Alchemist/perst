import java.util.Vector;
import javax.microedition.lcdui.*;
import org.garret.perst.fulltext.*;

public class SearchResultForm extends Form implements CommandListener  
{
    PIMindex main;
    SearchResultList list;

    public SearchResultForm(PIMindex main, FullTextSearchHit hit, SearchResultList list)
    {
        super("Rank " + hit.rank);
        this.main = main;
        this.list = list;
        Vector pairs = ((ContactDetails)hit.getDocument()).pairs;
        int nPairs = pairs.size();
        for (int i = 0; i < nPairs; i++) { 
            ContactDetails.Pair pair = (ContactDetails.Pair)pairs.elementAt(i);
            append(new StringItem(pair.label, pair.value));
        }
        setCommandListener(this);
        addCommand(PIMindex.BACK_CMD);
        Display.getDisplay(main).setCurrent(this);
    }
    
    public void commandAction(Command c, Displayable d)  
    {
        Display.getDisplay(main).setCurrent(list);
    }
}