package org.garret.perst;

/**
 * Tune parameter for SQL optimizer
 */
public class SqlOptimizerParameters
{ 
    /**
     * If optimization is enabled SQL engine will choose order of query conjuncts execution based 
     * on the estimation of their execution cost, if optimization is disable, 
     * then conjuncts will be executed in the same order as them are specified in the query.
     */
    public boolean enableCostBasedOptimization;

    /**
     * Index is not applicable
     */
    public int sequentialSearchCost;

    /**
     * Cost of searching using non-unique index. It is used only of equality comparisons and is added to eqCost, eqStringConst...
     */
    public int notUniqCost;
    
    /**
     * Cost of index search of key of scalar, reference or date type 
     */
    public int eqCost;
    
    /**
     * Cost of search in boolean index
     */
    public int eqBoolCost;

    /**
     * Cost of search in string index
     */
    public int eqStringCost;

    /**
     * Cost of search in real index
     */
    public int eqRealCost;

    /**
     * Cost for the following comparison operations: &lt; &lt;= &gt; &gt;=
     */
    public int openIntervalCost;
    
    /**
     * Cost for BETWEEN operation
     */
    public int closeIntervalCost;

    /**
     * Cost of index search of collection elements
     */
    public int containsCost;

    /**
     * Cost of boolean OR operator
     */
    public int orCost;

    /**
     * Cost of boolean AND operator
     */
    public int andCost;

    /**
     * Cost of IS NULL operator
     */
    public int isNullCost;


    /**
     * Cost of LIKE operator
     */
    public int patternMatchCost;

    
    /**
     * Cost of each extra level of indirection, for example in condition (x.y.z = 1) indirection level is 2 and in condition (x = 2) it is 0.
     */
    public int indirectionCost;

    /**
     * Default constructor setting default values of parameters
     */
    public SqlOptimizerParameters()
    {
        enableCostBasedOptimization = false;
        sequentialSearchCost = 1000;
        openIntervalCost = 100;
        containsCost = 50;
        orCost = 10;
        andCost = 10;
        isNullCost = 6;
        closeIntervalCost = 5;
        patternMatchCost = 2;
        eqCost = 1;
        eqRealCost = 2;
        eqStringCost = 3;
        eqBoolCost = 200;
        indirectionCost = 2;
        notUniqCost = 1;
    }
}
    