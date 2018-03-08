package org.garret.perst.assoc;

/**
 * Class used to specify desired order of result query
 */
public class OrderBy 
{
    /**
     * Sort order
     */
    public enum Order
    {
        Ascending,
        Descending
    };

    public final String name;
    public final Order  order;
   
    /**
     * Constructor of order-by component with default (ascending) sort order
     * @param name attribute name
     */
    public OrderBy(String name) { 
        this(name, Order.Ascending);
    }
    /**
     * Constructor of order-by component with specified sort order
     * @param name attribute name
     * @param order sort ascending or descebding sort order
     */    
    public OrderBy(String name, Order order) { 
        this.name = name;
        this.order = order;
    }
}