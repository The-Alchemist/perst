package org.garret.perst.continuous;

/**
 * Exception thown by full text search query parser
 */
public class FullTextSearchQuerySyntaxError extends ContinuousException 
{
    FullTextSearchQuerySyntaxError(Exception x) {
        super(x);
    }
}