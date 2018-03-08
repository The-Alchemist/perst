package org.garret.perst.assoc;

/**
 * Class used to construct search query
 */
public class Predicate 
{
    /**
     * Conjunction: Logical AND of two conditions
     */
    public static class And extends Predicate
    { 
        /**
         * Logical AND constructor
         * @param left left condition
         * @param right right condition
         */
        public And(Predicate left, Predicate right) { 
            this.left = left;
            this.right = right;
        }
        public final Predicate left;
        public final Predicate right;
    }

    /**
     * Disjunction: Logical OR of two conditions
     */
    public static class Or extends Predicate
    { 
        /**
         * Logical OR constructor
         * @param left left condition
         * @param right right condition
         */
        public Or(Predicate left, Predicate right) { 
            this.left = left;
            this.right = right;
        }
        public final Predicate left;
        public final Predicate right;
    }

    /**
     * Compare operation: compares attribute value with specified constant
     * Please notice that operation rather than Equals may requires sorting of selection of result
     * is used in logical AND or OR operations (it can influence on query execution time and memory demands)
     */
    public static class Compare extends Predicate
    {
        /**
         * Comparison operations
         */
        public enum Operation 
        {
            Equals,
            LessThan,
            LessOrEquals,
            GreaterThan,
            GreaterOrEquals,    
            StartsWith,
            IsPrefixOf,
            InArray
        }

        /**
         * Constructor of comparison operation
         * @param name attribute name
         * @param oper comparison operation
         * @param value compared value
         */
        public Compare(String name, Operation oper, Object value) { 
            this.name = name;
            this.oper = oper;
            this.value = value;
        }
        public final String    name;
        public final Operation oper;
        public final Object    value;
    }

    /**
     * Between operation
     */
    public static class Between extends Predicate 
    {
        /**
         * Constructor of between operation
         * @param name attribute name
         * @param from minimal value (inclusive)
         * @param till maximal value (inclusive)
         */
        public Between(String name, Object from, Object till) { 
            this.name = name;
            this.from = from;
            this.till = till;
        }
        public final String name;
        public final Object from;
        public final Object till;
    }

    /**
     * Equivalent of SQL IN operator
     */
    public static class In extends Predicate 
    {
        /**
         * Constructor of In operator
         * @param name attribute name
         * @param subquery nested subquery
         */
        public In(String name, Predicate subquery) { 
            this.name = name;
            this.subquery = subquery;
        }
        public final String name;
        public final Predicate subquery;
    }

    /**
     * Full text search query
     */
    public static class Match extends Predicate 
    {
        /**
         * Constructor of full text search query
         * @param query text of query. Full text index is able to execute search queries with logical operators (AND/OR/NOT) and 
         * strict match. Returned results are ordered by rank, which includes inverse document frequency (IDF),
         * frequency of word in the document, occurrence kind and nearness of query keywords in the document text
         * @param maxResults maximal amount of selected documents
         * @param timeLimit limit for query execution time
         */
        public Match(String query, int maxResults, int timeLimit) { 
            this.query = query;
            this.maxResults = maxResults;
            this.timeLimit = timeLimit;
        }
        public final String query;
        public final int maxResults;
        public final int timeLimit;
    }

    /**
     * Construct Logical AND operation
     * @param left left condition
     * @param right right condition
     */
    public static Predicate and(Predicate left, Predicate right) 
    { 
        return new And(left, right);
    }

    /**
     * Construct Logical OR operation
     * @param left left condition
     * @param right right condition
     */
    public static Predicate or(Predicate left, Predicate right) 
    { 
        return new Or(left, right);
    }

    /**
     * Construct comparison operation
     * @param name attribute name
     * @param oper comparison operation
     * @param value compared value
     */
    public static Predicate compare(String name, Predicate.Compare.Operation oper, Object value) 
    { 
        return new Compare(name, oper, value);
    }

    /**
     * Construct between operation
     * @param name attribute name
     * @param from minimal value (inclusive)
     * @param till maximal value (inclusive)
     */
    public static Predicate between(String name, Object from, Object till) 
    { 
        return new Between(name, from, till);
    }

    /**
     * Construct IN subquery
     * @param name attribute name
     * @param subquery nested subquery
     */
    public static Predicate in(String name, Predicate subquery) 
    { 
        return new In(name, subquery);
    }

    /**
     * Construct full text search query
     * @param query text of query. Full text index is able to execute search queries with logical operators (AND/OR/NOT) and 
     * strict match. Returned results are ordered by rank, which includes inverse document frequency (IDF),
     * frequency of word in the document, occurrence kind and nearness of query keywords in the document text
     * @param maxResults maximal amount of selected documents
     * @param timeLimit limit for query execution time
     */
    public static Predicate match(String query, int maxResults, int timeLimit) 
    { 
        return new Match(query, maxResults, timeLimit);
    }
}