package org.garret.perst.continuous;

import java.util.*;
import org.garret.perst.*;

class EmptyIterator<T> extends IterableIterator<T> 
{ 
    public boolean hasNext() 
    { 
        return false;
    }

    public T next() 
    {
        throw new NoSuchElementException();
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }
}
   