package org.garret.perst;

/**
 * Interface of persistent set. 
 */
public interface IPersistentSet extends IPersistent, IResource, Set 
{
    /**
     * Perform join of two sorted set. This method can be used to incrementally
     * join two or more inverse lists (represented using IPersistentSet collections).
     * For example, assume that we need to implement own primitive full text search engine
     * (please notice that Perst has builtin full text search engine).
     * So we have inverse index keyword-&gt;list of documents with occurrences of this keyword.
     * Given full text query (set of keywords) we need to locate all documents
     * which contains all specified keywords. It can be done in this way:
     * <code>
     * class FullTextSearchEngine extends PersistentResource {
     *     Index inverseIndex;
     *     public FullTextSearchEngine(Storage storage) { 
     *         super(storage);
     *         inverseIndex = storage.createIndex(Types.String, true);
     *     }
     *     public Iterator search(String[] keywords) {
     *         Iterator result = null;
     *         for (int i = 0; i < keywords.length; i++) { 
     *             String keyword = keywords[i];
     *             IPersistentSet occurrences = (IPersistentSet)inverseIndex.get(keyword); 
     *             if (occurrences == null) {
     *                 return null;
     *             }
     *             result = occurrences.join(result);
     *         }
     *         return result;
     *     }
     * }
     * </code>
     * 
     * @param iterator set of object ordered by OID. Usually it is iterator of IPersistentSet class.
     * This parameter may be null, in this case iterator of the target persistent set is returned.
     * @return iterator through result of two sets join. Join is performed incrementally so join
     * of two large sets will not consume a lot of memory and first results of such join
     * can be obtains fast enough.
     */
    public Iterator join(Iterator iterator);
}
