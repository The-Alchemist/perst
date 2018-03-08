package org.garret.perst.assoc;

import org.garret.perst.*;
import org.garret.perst.fulltext.*;
import java.io.StringReader;
import java.util.*;

/**
 * Read-write transaction.
 * AssocDB provides MURSIW (multiple readers single writer) isolation model.
 * It means that only one transaction can update the database at each moment of time, but multiple transactions
 * can concurrently read it.
 * 
 * All access to the database (read or write) should be performed within transaction body.
 * 
 * Transaction should be explicitly started by correspondent method of AssocDB and then it has to be either committed, 
 * either aborted. In any case, it can not be used any more after commit or rollback - you should start another transaction.
 */
public class ReadWriteTransaction extends ReadOnlyTransaction
{
    /**
     * Associate new string value with this item. If there are already some associations with this name for this item, then
     * new value will be appended at the end.
     * @param item source item
     * @param name attribute name (verb in terms of associative database model)
     * @param value attribute value
     */
    public void link(Item item, String name, String value) 
    { 
        checkIfActive();
        Index<Item> index = getIndex(name, String.class);
        int id = index.getOid();
        int l = 0, r = item.stringFields.length;
        while (l < r) {
            int m = (l + r) >>> 1;
            if (item.fieldIds[m] <= id) { // we want to locate position after all such IDs
                l = m + 1;
            } else { 
                r = m;
            }
        }
        index.put(new Key(value.toLowerCase()), item);
        item.fieldIds = Array.insert(item.fieldIds, r, id);
        item.stringFields = Array.insert(item.stringFields, r, value);
        modify(item);
    }

    /**
     * Associate new string value with this item at given position.
     * This operation is analog of insertion in an array at specified position.
     * @param item source item
     * @param name attribute name (verb in terms of associative database model)
     * @param value attribute value
     * @param position position at which new value should be inserted
     * @return true if value is successfully inserted or false if it can not be inserted at this position (position is more than one greater 
     * than "array" size)
     */
    public boolean linkAt(Item item, String name, String value, int position) 
    { 
        checkIfActive();
        Index<Item> index = getIndex(name, String.class);
        int id = index.getOid();
        int l = 0, n = item.stringFields.length, r = n;
        while (l < r) {
            int m = (l + r) >>> 1;
            if (item.fieldIds[m] < id) { 
                l = m + 1;
            } else { 
                r = m;
            }
        }
        if (position != 0) { 
            r += position;
            if (r > n || ((r == n || item.fieldIds[r] != id) && (r == 0 || item.fieldIds[r - 1] != id))) {
                return false;
            }
        }
        index.put(new Key(value.toLowerCase()), item);
        item.fieldIds = Array.insert(item.fieldIds, r, id);
        item.stringFields = Array.insert(item.stringFields, r, value);
        modify(item);
        return true;
    }

    /**
     * Associate new numeric value with this item. If there are already some associations with this name for this item, then
     * new value will be appended at the end.
     * @param item source item
     * @param name attribute name (verb in terms of associative database model)
     * @param value attribute value
     */
    public void link(Item item, String name, double value)
    {
        checkIfActive();
        Index<Item> index = getIndex(name, double.class);
        int id = index.getOid();
        int nStrings = item.stringFields.length;
        int l = nStrings, r = l + item.numericFields.length;
        while (l < r) {
            int m = (l + r) >>> 1;
            if (item.fieldIds[m] <= id) {  // we want to locate position after all such IDs
                l = m + 1;
            } else { 
                r = m;
            }
        }
        index.put(new Key(value), item);
        item.fieldIds = Array.insert(item.fieldIds, r, id);
        item.numericFields = Array.insert(item.numericFields, r - nStrings, value);
        modify(item);
    }

    /**
     * Associate new numeric value with this item at given position.
     * This operation is analog of insertion in an array at specified position.
     * @param item source item
     * @param name attribute name (verb in terms of associative database model)
     * @param value attribute value
     * @param position position at which new value should be inserted
     * @return true if value is successfully inserted or false if it can not be inserted at this position (position is more than one greater 
     * than "array" size)
     */
    public boolean linkAt(Item item, String name, double value, int position)
    {
        checkIfActive();
        Index<Item> index = getIndex(name, double.class);
        int id = index.getOid();
        int nStrings = item.stringFields.length;
        int l = nStrings, n = l + item.numericFields.length, r = n;
        while (l < r) {
            int m = (l + r) >>> 1;
            if (item.fieldIds[m] < id) { 
                l = m + 1;
            } else { 
                r = m;
            }
        }
        if (position != 0) { 
            r += position;
            if (r > n || ((r == n || item.fieldIds[r] != id) && (r == 0 || item.fieldIds[r - 1] != id))) {
                return false;
            }
        }
        index.put(new Key(value), item);
        item.fieldIds = Array.insert(item.fieldIds, r, id);
        item.numericFields = Array.insert(item.numericFields, r - nStrings, value);
        modify(item);
        return true;
    }

    /**
     * Associate several strings value with this item. If there are already some associations with this name for this item, then
     * new values will be appended at the end.
     * @param item source item
     * @param name attribute name (verb in terms of associative database model)
     * @param values array with attribute values
     */
    public void link(Item item, String name, String[] values)
    {
        checkIfActive();
        Index<Item> index = getIndex(name, String.class);
        int id = index.getOid();
        int l = 0, r = item.stringFields.length;
        while (l < r) {
            int m = (l + r) >>> 1;
            if (item.fieldIds[m] <= id) {  // we want to locate position after all such IDs
                l = m + 1;
            } else { 
                r = m;
            }
        }
        for (int i = 0; i < values.length; i++) { 
            index.put(new Key(values[i].toLowerCase()), item);
        }
        item.fieldIds = Array.insert(item.fieldIds, r, id, values.length);
        item.stringFields = Array.insert(item.stringFields, r, values);
        modify(item);
    }

    /**
     * Associate several numeric value with this item. If there are already some associations with this name for this item, then
     * new values will be appended at the end.
     * @param item source item
     * @param name attribute name (verb in terms of associative database model)
     * @param values array with attribute values
     */
    public void link(Item item, String name, double[] values)
    {
        checkIfActive();
        Index<Item> index = getIndex(name, double.class);
        int id = index.getOid();
        int nStrings = item.stringFields.length;
        int l = nStrings, n = l + item.numericFields.length, r = n;
        while (l < r) {
            int m = (l + r) >>> 1;
            if (item.fieldIds[m] <= id) {  // we want to locate position after all such IDs
                l = m + 1;
            } else { 
                r = m;
            }
        }
        for (int i = 0; i < values.length; i++) { 
            index.put(new Key(values[i]), item);
        }
        item.fieldIds = Array.insert(item.fieldIds, r, id, values.length);
        item.numericFields = Array.insert(item.numericFields, r - nStrings, values);
        modify(item);
    }

    /**
     * Add relation to another item. Relation can be either embedded inside object (item) - if total number of links from the
     * item doesn't reach embedded relation threshold, either stores in separate B-Tree index.
     * AssocDB automatically adds to the target item inverse link - attribute with name "-XXX" where XXX is specified attribute name.
     * Inverse links are needed for preserving references consistency in case of updates/deletes but them can be also
     * used by application for traversal between objects. But You should not try to explicitly update or delete inverse reference.
     * @param item source item
     * @param name attribute name (verb in terms of associative database model)
     * @param target target item
     */
    public void link(Item item, String name, Item target)
    {
        checkIfActive();
        checkIfNotInverseLink(name);
        addLink(item, name, target);
        addLink(target, "-" + name, item);
    }

    /**
     * Add relation to another item. 
     * This operation is analog of insertion in an array at specified position.
     * This operation will fail of relations for this item are already stored in external (non-embedded) way.
     * 
     * And unlike <code>link(Item item, String name, Item target)</code> method this 
     * method never cause change if embedded relation representation to external one even if
     * total number of links from this item exceeds threshold value for embedded relations.
     * 
     * AssocDB automatically adds to the target item inverse link - attribute with name "-XXX" where XXX is specified attribute name.
     * Inverse links are needed for preserving references consistency in case of updates/deletes but them can be also
     * used by application for traversal between objects. But You should not try to explicitly update or delete inverse reference.
     * @param item source item
     * @param name attribute name (verb in terms of associative database model)
     * @param target target item
     * @param position position at which new value should be inserted
     * @return true if value is successfully inserted or false if it can not be inserted at this position (position is more than one greater 
     * than "array" size)
     */
    public boolean linkAt(Item item, String name, Item target, int position)
    {
        checkIfActive();
        checkIfNotInverseLink(name);
        Index<Item> index = getIndex(name, Object.class);
        int id = index.getOid();
        int nStrings = item.stringFields.length;
        int nNumbers = item.numericFields.length;
        int nFields = item.fieldIds.length;

        if (item.relations == null) { 
            return false;
        }
        int l = nStrings + nNumbers, r = nFields;
        while (l < r) {
            int m = (l + r) >>> 1;
            if (item.fieldIds[m] < id) { 
                l = m + 1;
            } else { 
                r = m;
            }
        }
        if (position != 0) { 
            r += position;
            if (r > nFields || ((r == nFields || item.fieldIds[r] != id) && (r == 0 || item.fieldIds[r - 1] != id))) {
                return false;
            }
        }
        index.put(new Key(target), item);
        item.fieldIds = Array.insert(item.fieldIds, r, id);
        item.relations.insert(r - nStrings - nNumbers, target);
        modify(item);
        addLink(target, "-" + name, item);
        return true;
    }

    /**
     * Add relation to another items. Relation can be either embedded inside object (item) - if total number of links from the
     * item doesn't reach embedded relation threshold, either stores in separate B-Tree index.
     * @param item source item
     * @param name attribute name (verb in terms of associative database model)
     * @param targets array with related items
     */
    public void link(Item item, String name, Item[] targets)
    {
        checkIfActive();
        Index<Item> index = getIndex(name, Object.class);
        int id = index.getOid();
        int nStrings = item.stringFields.length;
        int nNumbers = item.numericFields.length;
        for (int i = 0; i < targets.length; i++) { 
            index.put(new Key(targets[i]), item);
        }
        if (item.relations != null && item.relations.size() + targets.length > db.embeddedRelationThreshold) { 
            int j = nStrings + nNumbers;
            for (int i = 0, n = item.relations.size(); i < n; i++) { 
                addRelation(item, item.fieldIds[i+j], item.relations.get(i));
            }
            item.fieldIds = Array.truncate(item.fieldIds, j);
            item.relations = null;
            modify(item);
        }
        if (item.relations != null) { 
            int nFields = item.fieldIds.length;
            int l = nStrings + nNumbers, r = nFields;
            while (l < r) {
                int m = (l + r) >>> 1;
                if (item.fieldIds[m] <= id) { 
                    l = m + 1;
                } else { 
                    r = m;
                }
            }
            item.fieldIds = Array.insert(item.fieldIds, r, id, targets.length);
            for (int i = r - nStrings - nNumbers, j = 0; j < targets.length; j++) { 
                item.relations.insert(i + j, targets[j]);
            }
            modify(item);
        } else { 
            for (int i = 0; i < targets.length; i++) { 
                addRelation(item, id, targets[i]);
            }
            item.fieldNames = null;
        }
        for (int i = 0; i < targets.length; i++) { 
            addLink(targets[i], "-" + name, item);
        }
    }

    /**
     * Associate new value with this item. If there are already some associations with this name for this item, then
     * new value will be appended at the end.
     * @param item source item
     * @param name attribute name (verb in terms of associative database model)
     * @param any attribute value (should be of String, Double/Number or Item type)
     */
    public void link(Item item, String name, Object any)
    {
        if (any instanceof String) { 
            link(item, name, (String)any);
        } else if (any instanceof Number) { 
            link(item, name, ((Number)any).doubleValue());
        } else { 
            link(item, name, (Item)any);
        }
    }

    /**
     * Associate new value with this item at given position.
     * This operation is analog of insertion in an array at specified position.
     * @param item source item
     * @param name attribute name (verb in terms of associative database model)
     * @param any attribute value (should be of String, Double/Number or Item type)
     * @param position position at which new value should be inserted
     * @return true if value is successfully inserted or false if it can not be inserted at this position (position is more than one greater 
     * than "array" size)
     */
    public boolean linkAt(Item item, String name, Object any, int position)
    {
        return (any instanceof String) 
            ? linkAt(item, name, (String)any, position)
            : (any instanceof Number) 
                ? linkAt(item, name, ((Number)any).doubleValue(), position)
                : linkAt(item, name, (Item)any, position);
    }

    /**
     * Associate new name-value pair with this item
     * @param item source item
     * @param pair name-value pair
     */
    public void link(Item item, Pair pair)
    {
        link(item, pair.name, pair.value);
    }

    /**
     * Associate new name-value pair with this item at given position
     * This operation is analog of insertion in an array at specified position.
     * @param item source item
     * @param pair name-value pair
     * @param position position at which new value should be inserted
     * @return true if value is successfully inserted or false if it can not be inserted at this position (position is more than one greater 
     * than "array" size)
     */
    public boolean linkAt(Item item, Pair pair, int position)
    {
        return linkAt(item, pair.name, pair.value, position);
    }

    /**
     * Associate new name-value pairs with this item
     * @param item source item
     * @param pairs array of name-value pair
     */
    public void link(Item item, Pair[] pairs)
    {
        for (int i = 0; i < pairs.length; i++) { 
            link(item, pairs[i]);
        }
    }

    /**
     * Update association with string value: replace old attribute value with new one
     * If there are no attributes with the given name associated with this item, then this method just adds new association.
     * If there are more than one associations with the given name, then first of them is updated
     * @param item source item
     * @param name attribute name (verb in terms of associative database model)
     * @param value new attribute value
     */
    public void update(Item item, String name, String value) 
    {
        checkIfActive();
        Index<Item> index = getIndex(name, String.class);
        int id = index.getOid();
        int l = 0, n = item.stringFields.length, r = n;
        while (l < r) {
            int m = (l + r) >>> 1;
            if (item.fieldIds[m] < id) { 
                l = m + 1;
            } else { 
                r = m;
            }
        }
        index.put(new Key(value.toLowerCase()), item);
        if (r < n && item.fieldIds[r] == id) { 
            index.remove(new Key(item.stringFields[r].toLowerCase()), item);
            item.stringFields[r] = value;
        } else { 
            item.fieldIds = Array.insert(item.fieldIds, r, id);
            item.stringFields = Array.insert(item.stringFields, r, value);
        }
        modify(item);
    }

    /**
     * Update association with string value at the specified position
     * This operation is analog of replacing an array element
     * @param item source item
     * @param name attribute name (verb in terms of associative database model)
     * @param value new attribute value
     * @param position position at which new value should be inserted
     * @return true if value is successfully updated, false if specified position doesn't belong to "array"
     */
    public boolean updateAt(Item item, String name, String value, int position) 
    { 
        checkIfActive();
        Index<Item> index = getIndex(name, String.class);
        int id = index.getOid();
        int l = 0, n = item.stringFields.length, r = n;
        while (l < r) {
            int m = (l + r) >>> 1;
            if (item.fieldIds[m] < id) { 
                l = m + 1;
            } else { 
                r = m;
            }
        }
        r += position;
        if (r < n && item.fieldIds[r] == id) { 
            index.remove(new Key(item.stringFields[r].toLowerCase()), item);
            index.put(new Key(value.toLowerCase()), item);
            item.stringFields[r] = value;
            modify(item);
            return true;
        } else { 
            return false;
        }
    }

    /**
     * Update association with numeric value: replace old attribute value with new one
     * If there are no attributes with the given name associated with this item, then this method just adds new association.
     * If there are more than one associations with the given name, then first of them is updated
     * @param item source item
     * @param name attribute name (verb in terms of associative database model)
     * @param value new attribute value
     */
    public void update(Item item, String name, double value)
    {
        checkIfActive();
        Index<Item> index = getIndex(name, double.class);
        int id = index.getOid();
        int nStrings = item.stringFields.length;
        int l = nStrings, r = l + item.numericFields.length, n = r;
        while (l < r) {
            int m = (l + r) >>> 1;
            if (item.fieldIds[m] < id) { 
                l = m + 1;
            } else { 
                r = m;
            }
        }
        index.put(new Key(value), item);
        if (r < n && item.fieldIds[r] == id) { 
            r -= nStrings;
            index.remove(new Key(item.numericFields[r]), item);
            item.numericFields[r] = value;
        } else { 
            item.fieldIds = Array.insert(item.fieldIds, r, id);
            item.numericFields = Array.insert(item.numericFields, r - nStrings, value);
        }
        modify(item);
    }

    /**
     * Update association with numeric value at the specified position
     * This operation is analog of replacing an array element
     * @param item source item
     * @param name attribute name (verb in terms of associative database model)
     * @param value new attribute value
     * @param position position at which new value should be inserted
     * @return true if value is successfully updated, false if specified position doesn't belong to "array"
     */
    public boolean updateAt(Item item, String name, double value, int position)
    {
        checkIfActive();
        Index<Item> index = getIndex(name, double.class);
        int id = index.getOid();
        int nStrings = item.stringFields.length;
        int l = nStrings, r = l + item.numericFields.length, n = r;
        while (l < r) {
            int m = (l + r) >>> 1;
            if (item.fieldIds[m] < id) { 
                l = m + 1;
            } else { 
                r = m;
            }
        }
        r += position;
        if (r < n && item.fieldIds[r] == id) { 
            r -= nStrings;
            index.put(new Key(value), item);
            index.remove(new Key(item.numericFields[r]), item);
            item.numericFields[r] = value;
            modify(item);
            return true;
        } else { 
            return false;
        }
    }

    /**
     * Update association with other item at the specified position
     * This operation is analog of replacing an array element
     * @param item source item
     * @param name attribute name (verb in terms of associative database model)
     * @param target new associated item
     * @param position position at which new value should be inserted
     * @return true if value is successfully updated, false if relation is not embedded or specified position doesn't belong to "array"
     */
    public boolean updateAt(Item item, String name, Item target, int position)
    {
        checkIfActive();
        Index<Item> index = getIndex(name, Object.class);
        int id = index.getOid();
        int offs = item.stringFields.length + item.numericFields.length;
        if (item.relations == null) { 
            return false;
        }
        int l = offs, n = item.fieldIds.length, r = n;
        while (l < r) {
            int m = (l + r) >>> 1;
            if (item.fieldIds[m] < id) { 
                l = m + 1;
            } else { 
                r = m;
            }
        }
        r += position;
        if (r < n && item.fieldIds[r] == id) { 
            r -= offs;
            index.put(new Key(target), item);
            removeLink(item, item.relations.get(r), id);
            item.relations.set(r, target);
            addLink(target, "-" + name, item);
            modify(item);
            return true;
        } else { 
            return false;
        }
    }

    /**
     * Update association: replace old attribute value with new one
     * If there are no attributes with the given name associated with this item, then this method just adds new association.
     * If there are more than one associations with the given name, then first of them is updated
     * @param item source item
     * @param name attribute name (verb in terms of associative database model)
     * @param any new attribute value
     */
    public void update(Item item, String name, Object any)
    {
        if (any instanceof String) { 
            update(item, name, (String)any);
        } else { 
            update(item, name, ((Number)any).doubleValue());
        }
    }

    /**
     * Update association at the specified position
     * This operation is analog of replacing an array element
     * @param item source item
     * @param name attribute name (verb in terms of associative database model)
     * @param any new attribute value
     * @param position position at which new value should be inserted
     * @return true if value is successfully updated, false if specified position doesn't belong to "array"
     */
    public boolean updateAt(Item item, String name, Object any, int position)
    {
        return (any instanceof String)
            ? updateAt(item, name, (String)any, position)
            : (any instanceof Number) 
                ? updateAt(item, name, ((Number)any).doubleValue(), position)
                : updateAt(item, name, (Item)any, position);
    }

    /**
     * Update association using given name-value pair
     * If there are no attributes with the given name associated with this item, then this method just adds new association.
     * If there are more than one associations with the given name, then first of them is updated
     * @param item source item
     * @param pair name-value pair
     */
    public void update(Item item, Pair pair)
    {
        update(item, pair.name, pair.value);
    }

    /**
     * Update association using given name-value pair
     * This operation is analog of replacing an array element
     * @param item source item
     * @param pair name-value pair
     * @param position position at which new value should be inserted
     * @return true if value is successfully updated, false if specified position doesn't belong to "array"
     */
    public boolean updateAt(Item item, Pair pair, int position)
    {
        return updateAt(item, pair.name, pair.value, position);
    }

    /**
     * Update association using given array of name-value pairs
     * If there are no attributes with the given name associated with this item, then this method just adds new associations.
     * If there are more than one associations with the given name, then first of them is updated
     * @param item source item
     * @param pairs array of name-value pairs
     */
    public void update(Item item, Pair[] pairs)
    {
        for (int i = 0; i < pairs.length; i++) { 
            update(item, pairs[i]);
        }
    }
    
    /**
     * Remove association with string value for this item.
     * @param item source item
     * @param name attribute name (verb in terms of associative database model)
     * @param value attribute value
     * @return true if association with such name and value exists for this item, false otherwise
     */
    public boolean unlink(Item item, String name, String value)
    {
        checkIfActive();
        Integer idWrapper = db.name2id.get(name);
        if (idWrapper == null) { 
            return false;
        }        
        int id = idWrapper.intValue();              
        int l = 0, n = item.stringFields.length, r = n;
        while (l < r) { 
            int m = (l + r) >>> 1;
            if (item.fieldIds[m] < id) { 
                l = m + 1;
            } else { 
                r = m;
            }
        }
        while (r < n && item.fieldIds[r] == id) { 
            if (item.stringFields[r].equals(value)) { 
                item.stringFields = Array.remove(item.stringFields, r);
                item.fieldIds = Array.remove(item.fieldIds, r);
                modify(item);
                Index<Item> index = (Index<Item>)db.storage.getObjectByOID(id);
                index.remove(new Key(value.toLowerCase()), item);
                return true;
            } 
            r += 1;
        }
        return false;
    }

    /**
     * Remove association with numeric value for this item.
     * @param item source item
     * @param name attribute name (verb in terms of associative database model)
     * @param value attribute value
     * @return true if association with such name and value exists for this item, false otherwise
     */
    public boolean unlink(Item item, String name, double value)
    {
        checkIfActive();
        Integer idWrapper = db.name2id.get(name);
        if (idWrapper == null) { 
            return false;
        }
        int id = idWrapper.intValue();
        int nStrings =  item.stringFields.length;
        int l = nStrings, r = l + item.numericFields.length, n = r;
        while (l < r) { 
            int m = (l + r) >>> 1;
            if (item.fieldIds[m] < id) { 
                l = m + 1;
            } else { 
                r = m;
            }
        }
        while (r < n && item.fieldIds[r] == id) { 
            if (item.numericFields[r - nStrings] == value) { 
                item.fieldIds = Array.remove(item.fieldIds, r);
                item.stringFields = Array.remove(item.stringFields, r - nStrings);
                modify(item);
                Index<Item> index = (Index<Item>)db.storage.getObjectByOID(id);
                index.remove(new Key(value), item);
                return true;
            } 
            r += 1;
        }
        return false;
    }

    /**
     * Remove association with value for this item.
     * @param item source item
     * @param name attribute name (verb in terms of associative database model)
     * @param any attribute value
     * @return true if association with such name and value exists for this item, false otherwise
     */
    public boolean unlink(Item item, String name, Object any) 
    {
        return (any instanceof String)  
            ? unlink(item, name, (String)any)
            : (any instanceof Number) 
                ? unlink(item, name, ((Number)any).doubleValue())
                : unlink(item, name, (Item)any);
    }
    
    /**
     * Remove association specified by name-value pair for this item.
     * @param item source item
     * @param pair name-value pair
     * @return true if association with such name and value exists for this item, false otherwise
     */
    public boolean unlink(Item item, Pair pair) 
    { 
        return unlink(item, pair.name, pair.value);
    }

    /**
     * Remove associations specified by array of name-value pair for this item.
     * @param item source item
     * @param pairs array of name-value pairs
     * @return number of removed associations 
     */
    public int unlink(Item item, Pair[] pairs) 
    { 
        int nUnlinked = 0;
        for (int i = 0; i < pairs.length; i++) { 
            if (unlink(item, pairs[i].name, pairs[i].value)) { 
                nUnlinked += 1;
            }
        }
        return nUnlinked;
    }
    
    /**
     * Remove association between two items
     * @param item source item
     * @param name attribute name (verb in terms of associative database model)
     * @param target target item
     * @return true if association between this two items exists, false otherwise
     */
    public boolean unlink(Item item, String name, Item target)
    {
        checkIfActive();
        checkIfNotInverseLink(name);
        Integer idWrapper = db.name2id.get(name);
        if (idWrapper == null) { 
            return false;
        }
        int id = idWrapper.intValue();
        if (item.relations != null) { 
            int offs = item.stringFields.length + item.numericFields.length;
            int l = offs, n = item.fieldIds.length, r = n;
            while (l < r) { 
                int m = (l + r) >>> 1;
                if (item.fieldIds[m] < id) { 
                    l = m + 1;
                } else { 
                    r = m;
                }
            }
            while (r < n && item.fieldIds[r] == id) { 
                if (target.equals(item.relations.getRaw(r - offs))) { 
                    item.relations.removeObject(r - offs);                    
                    item.fieldIds = Array.remove(item.fieldIds, r);
                    modify(item);                    
                    removeLink(item, target, id);
                    return true;
                }
                r += 1;
            }
        } else { 
            if (db.root.relations.unlink(new Key(((long)item.getOid() << 32) | id), target)) { 
                removeLink(item, target, id);                                
                item.fieldNames = null; 
                return true;
            }
        }
        return false;
    }

    /**
     * Remove all associations with this name
     * @param item source item
     * @param name attribute name (verb in terms of associative database model)
     * @return number of removed associations
     */
    public int unlink(Item item, String name)
    {
        checkIfActive();
        Integer idWrapper = db.name2id.get(name);
        if (idWrapper == null) { 
            return 0;
        }
        int id = idWrapper.intValue();
        int nFields = item.fieldIds.length;
        int nStrings = item.stringFields.length;
        int nNumbers = item.numericFields.length;
        int l, r;
        for (l = 0; l < nFields && item.fieldIds[l] != id; l++);
        for (r = l; r < nFields && item.fieldIds[r] == id; r++);
        if (l < r) { 
            Index<Item> index = (Index<Item>)db.storage.getObjectByOID(id);
            if (l < nStrings) { 
                for (int i = l; i < r; i++) { 
                    index.remove(new Key(item.stringFields[i].toLowerCase()), item);
                }
                item.stringFields = Array.remove(item.stringFields, l, r-l);
            } else if (l < nStrings + nNumbers) { 
                for (int i = l; i < r; i++) { 
                    index.remove(new Key(item.numericFields[i-nStrings]), item);
                }
                item.numericFields =  Array.remove(item.numericFields, l-nStrings, r-l);
            } else {    
                checkIfNotInverseLink(name);
                int j = l - nStrings - nNumbers;
                for (int i = l; i < r; i++) { 
                    removeLink(item, item.relations.get(j), id);
                    item.relations.remove(j);
                }
            }
            item.fieldIds = Array.remove(item.fieldIds, l, r-l);            
            modify(item);
            return r-l;
        }
        checkIfNotInverseLink(name);
        Key key = new Key(((long)item.getOid() << 32) | id);
        int nUnlinked = 0;
        IterableIterator<Item> iterator = db.root.relations.iterator(key, key, Index.ASCENT_ORDER);
        while (iterator.hasNext()) { 
            Item target = iterator.next();
            iterator.remove();
            removeLink(item, target, id);                                
            nUnlinked += 1;
        }
        item.fieldNames = null;                    
        return nUnlinked;
    }
                
    /**
     * Remove all associations with this name at specified position.
     * This operation is analog to remove of element from an array
     * @param item source item
     * @param name attribute name (verb in terms of associative database model)
     * @param position remove position
     * @return true if position belongs to the "array", false otherwise
     */
    public boolean unlinkAt(Item item, String name, int position)
    {
        checkIfActive();
        Integer idWrapper = db.name2id.get(name);
        if (idWrapper == null) { 
            return false;
        }
        int id = idWrapper.intValue();
        int nFields = item.fieldIds.length;
        int nStrings = item.stringFields.length;
        int nNumbers = item.numericFields.length;
        int i;
        for (i = 0; i < nFields && item.fieldIds[i] != id; i++);
        i += position;
        if (i < nFields) { 
            Index<Item> index = (Index<Item>)db.storage.getObjectByOID(id);
            item.fieldIds = Array.remove(item.fieldIds, i);            
            if (i < nStrings) { 
                index.remove(new Key(item.stringFields[i].toLowerCase()), item);
                item.stringFields = Array.remove(item.stringFields, i);
            } else if ((i -= nStrings) < nNumbers) { 
                index.remove(new Key(item.numericFields[i]), item);
                item.numericFields =  Array.remove(item.numericFields, i);
            } else {    
                checkIfNotInverseLink(name);
                i -= nNumbers;
                removeLink(item, item.relations.get(i), id);
                item.relations.remove(i);
            }
            modify(item);
            return true;
        }
        return false;
    }
                
    /**
     * Create new item.
     * You can use <code>link</code> method to add association for this item
     */
    public Item createItem() 
    { 
        checkIfActive();
        return db.createItem();
    }

    /**
     * Rename a verb. This methods allows to change attribute name in all items.
     * As far as object format in AssocDB is completely dynamic, database schema evaluation doesn't require
     * to support any other changes rather than renaming of attributes.
     * Renaming may be necessary because of two reasons:
     * <ol>
     * <li>name conflict: assume that you have "time" attribute and later you realize that you need to support
     * two kind of times: creation time and last modification time</li>
     * <li>type conflict: AssocDB requires that values of attribute in all objects have the same type.
     * You can not store for example "age" in  one item as number 35 and in other item - as string "5 months".
     * If it is really needed you have to introduce new attribute and may be rename existed.</li>     
     * </ol>
     * Renaming should be performed before any invocation of Item.getAttributeNames() method which can cache old attribute name.
     * @param oldName old attribute name
     * @param newName new attribute name
     * @return true if rename is successfully performed, false if operation failed either because 
     * there is no oldName attribute in the database, either because newName attribute already exists
     */
    public boolean rename(String oldName, String newName) 
    { 
        checkIfActive();
        Integer idWrapper = db.name2id.get(oldName);
        if (idWrapper == null || db.name2id.get(newName) != null) { 
            return false;
        }
        db.name2id.remove(oldName);
        db.name2id.put(newName, idWrapper);
        db.id2name.put(idWrapper, newName);
        
        Index<Item> index = (Index<Item>)db.storage.getObjectByOID(idWrapper.intValue());
        db.root.attributes.remove(new Key(oldName), index);
        db.root.attributes.put(new Key(newName), index);
        return true;
    }

    /**
     * Remove item from the database.
     * This methods destroy all relations between this item and other items
     * @param item removed item
     */
    public void remove(Item item) 
    { 
        checkIfActive();
        int nStrings = item.stringFields.length;
        for (int i = 0; i < nStrings; i++) { 
            Index<Item> index = (Index<Item>)db.storage.getObjectByOID(item.fieldIds[i]);
            index.remove(new Key(item.stringFields[i].toLowerCase()), item);
        }
        int nNumbers = item.numericFields.length;
        for (int i = 0; i < nNumbers; i++) { 
            Index<Item> index = (Index<Item>)db.storage.getObjectByOID(item.fieldIds[i + nStrings]);
            index.remove(new Key(item.numericFields[i]), item);
        }            
        excludeFromFullTextIndex(item);

        if (item.relations != null) {             
            int nFields = item.fieldIds.length;
            for (int i = nStrings + nNumbers, j = 0; i < nFields; i++, j++) { 
                removeLink(item, item.relations.get(j), item.fieldIds[i]);
            }
        } else { 
            long oid = item.getOid();
            Iterator<Map.Entry<Object,Item>> iterator = db.root.relations.entryIterator(new Key(oid << 32, true), new Key((oid+1) << 32, false), Index.ASCENT_ORDER);
            while (iterator.hasNext())
            {
                Map.Entry<Object,Item> e = iterator.next();
                removeLink(item, e.getValue(), (int)((Long)e.getKey()).longValue());                                
                iterator.remove();
            }
        }
        item.deallocate();
    }

    /**
     * Include specified attributes of the item in full text index. 
     * Text of the item is assumed to be in default database language(set by AssocDB.setLanguage method)
     * All previous occurrences of the item in the index are removed.
     * To completely exclude item from the index it is enough to specify empty list of attributes.
     * @param item item included in full text index
     * @param attributeNames attributes to be included in full text index
     */
    public void includeInFullTextIndex(Item item, String[] attributeNames) 
    {
        includeInFullTextIndex(item, attributeNames, db.language); 
    }

    /**
     * Include specified attributes of the item in full text index.
     * All previous occurrences of the item in the index are removed.
     * To completely exclude item from the index it is enough to specify empty list of attributes.
     * @param item item included in full text index
     * @param language text language
     * @param attributeNames attributes to be included in full text index
     */
    public void includeInFullTextIndex(Item item, String[] attributeNames, String language) 
    {
        if (attributeNames == null || attributeNames.length == 0) { 
            excludeFromFullTextIndex(item);
        } else { 
            StringBuffer buf = new StringBuffer();
            for (int i = 0; i < attributeNames.length; i++) { 
                if (i != 0) { 
                    buf.append("; ");
                }
                Object val = item.getAttribute(attributeNames[i]);
                if (val != null) { 
                    if (val instanceof String[]) { 
                        String[] arr = (String[])val;
                        for (int j = 0; j < arr.length; j++) { 
                            if (j != 0) { 
                                buf.append(", ");
                            }
                            buf.append(arr[j]);
                        }
                    } else if (val instanceof double[]) { 
                        double[] arr = (double[])val;
                        for (int j = 0; j < arr.length; j++) { 
                            if (j != 0) { 
                                buf.append(", ");
                            }
                            buf.append(Double.toString(arr[j]));
                        }
                    } else { 
                        buf.append(val.toString());
                    }
                }
            }
            db.root.fullTextIndex.add(item, new StringReader(buf.toString()), language);
        }
    }

    /**
     * Include all string attributes of the item in full text index. 
     * Text of the item is assumed to be in default database language(set by AssocDB.setLanguage method)
     * @param item item included in full text index
     */
    public void includeInFullTextIndex(Item item) 
    {
        includeInFullTextIndex(item, db.language);
    }

    /**
     * Include all string attributes of the item in full text index.
     * @param item item included in full text index
     * @param language text language
     */
    public void includeInFullTextIndex(Item item, String language) 
    {
        StringBuffer buf = new StringBuffer();
        for (int i = 0, n = item.stringFields.length; i < n; i++) { 
            if (i != 0) { 
                buf.append(", ");
            }
            buf.append(item.stringFields[i]);
        }
        db.root.fullTextIndex.add(item, new StringReader(buf.toString()), language);       
    }

    /**
     * Exclude item from full text index
     * @param item item to be excluded from full text index
     */
    public void excludeFromFullTextIndex(Item item)
    {
        db.root.fullTextIndex.delete(item);
    }

    /**
     * Commit this transaction.
     * It is not possible to use this transaction object after it is committed
     */
    public void commit() 
    {        
        db.storage.commit(); 
        super.commit();
    }

    /**
     * Rollback this transaction (undo all changes done by this transaction)
     * It is not possible to use this transaction object after it is rollbacked
     */
    public void rollback() 
    { 
        db.storage.rollback();
        super.rollback();
    }
       

    final void removeLink(Item source, Item target, int id) 
    { 
        Index<Item> index = (Index<Item>)db.storage.getObjectByOID(id);
        index.remove(new Key(target), source);
        if (target.relations != null) {             
            int[] fieldIds = target.fieldIds;
            int nFields = fieldIds.length;
            int nDeleted = 0;
            for (int i = target.stringFields.length + target.numericFields.length, j = 0; i < nFields; i++) { 
                if (!source.equals(target.relations.getRaw(j))) { 
                    target.fieldIds[i-nDeleted] = fieldIds[i];
                    j += 1;
                } else { 
                    index = (Index<Item>)db.storage.getObjectByOID(fieldIds[i]);
                    index.remove(new Key(source), target);
                    target.relations.removeObject(j);
                    nDeleted += 1;
                }
            }
            if (nDeleted > 0) { 
                fieldIds = new int[nFields -= nDeleted];
                System.arraycopy(target.fieldIds, 0, fieldIds, 0, nFields);
                target.fieldIds = fieldIds;
                modify(target);
            }
        } else { 
            String name = db.id2name.get(id);
            int inverseId = db.name2id.get(name.startsWith("-") ? name.substring(1) : ("-" + name)).intValue();
            index = (Index<Item>)db.storage.getObjectByOID(inverseId);
            index.remove(new Key(source), target);
            target.fieldNames = null;
            db.root.relations.remove(new Key(((long)target.getOid() << 32) | inverseId), source);
        }
    }

    final void addRelation(Item item, int id, Item target) 
    { 
        db.root.relations.put(new Key(((long)item.getOid() << 32) | id), target);
    }

    final void addLink(Item item, String name, Item target)
    {
        Index<Item> index = getIndex(name, Object.class);
        index.put(new Key(target), item);
        int id = index.getOid();
        int nStrings = item.stringFields.length;
        int nNumbers = item.numericFields.length;

        if (item.relations != null && item.relations.size() >= db.embeddedRelationThreshold) { 
            int[] fieldIds = new int[nStrings + nNumbers];
            System.arraycopy(item.fieldIds, 0, fieldIds, 0, nStrings + nNumbers);
            int j = nStrings + nNumbers;
            for (int i = 0, n = item.relations.size(); i < n; i++) { 
                addRelation(item, item.fieldIds[i+j], item.relations.get(i));
            }
            item.fieldIds = fieldIds;
            item.relations = null;
            modify(item);
        }
        if (item.relations != null) { 
            int l = nStrings + nNumbers, offs = l, r = item.fieldIds.length;
            while (l < r) {
                int m = (l + r) >>> 1;
                if (item.fieldIds[m] <= id) { 
                    l = m + 1;
                } else { 
                    r = m;
                }
            }
            item.fieldIds = Array.insert(item.fieldIds, r, id);            
            item.relations.insert(r - offs, target);
            modify(item);
        } else { 
            addRelation(item, id, target);
            item.fieldNames = null;
        }
    }

    final Index<Item> getIndex(String name, Class cls) 
    { 
        Index<Item> index;
        Integer id = db.name2id.get(name);
        if (id == null) { 
            index = db.storage.<Item>createThickIndex(cls);
            db.root.attributes.put(name, index);
            id = new Integer(index.getOid());
            db.name2id.put(name, id);
            db.id2name.put(id, name);
        } else { 
            index = (Index<Item>)db.storage.getObjectByOID(id.intValue());
            if (!cls.equals(index.getKeyType())) {
                throw new IllegalArgumentException("Type conflict for keyword " + name + ": " + cls + " vs. " +  index.getKeyType());
            }
        }
        return index;
    }

    final void checkIfNotInverseLink(String name) 
    {
        if (name.startsWith("-")) { 
            throw new IllegalArgumentException("Inverse links can not be excplitely updated");
        }
    }

    final void modify(Item item) 
    { 
        item.fieldNames = null;
        item.modify();
    }


    protected ReadWriteTransaction(AssocDB db) 
    { 
        super(db);
    }
}