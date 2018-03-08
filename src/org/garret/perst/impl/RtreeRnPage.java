package org.garret.perst.impl;

import org.garret.perst.*;
import java.util.ArrayList;

public class RtreeRnPage extends Persistent implements SelfSerializable
{
    int           n;
    int           card;
    RectangleRn[] b;
    Link          branch;

    public void pack(PerstOutputStream out) throws java.io.IOException
    {
        int nDims = ((Page.pageSize-ObjectHeader.sizeof-12)/card - 4) / 16;
        out.writeInt(n);
        out.writeObject(branch);
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < nDims; j++) {
                out.writeDouble(b[i].getMinCoord(j));
                out.writeDouble(b[i].getMaxCoord(j));
            }
        }
    }

    public void unpack(PerstInputStream in) throws java.io.IOException
    {
        n = in.readInt();
        branch = (Link)in.readObject();
        card = branch.size();
        int nDims = ((Page.pageSize-ObjectHeader.sizeof-12)/card - 4) / 16;
        double[] coords = new double[nDims*2];
        b = new RectangleRn[card];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < nDims; j++) {
                coords[j] = in.readDouble();
                coords[j+nDims] = in.readDouble();
            }
            b[i] = new RectangleRn(coords);
        }
    }

    RtreeRnPage(Storage storage, Object obj, RectangleRn r) {
        card = (Page.pageSize-ObjectHeader.sizeof-12)/(16*r.nDimensions()+4);
        branch = storage.createLink(card);
        branch.setSize(card);
        b = new RectangleRn[card];
        setBranch(0, new RectangleRn(r), obj);
        n = 1;
    }

    RtreeRnPage(Storage storage, RtreeRnPage root, RtreeRnPage p) {
        card = root.card;
        branch = storage.createLink(card);
        branch.setSize(card);
        b = new RectangleRn[card];
        n = 2;
        setBranch(0, root.cover(), root);
        setBranch(1, p.cover(), p);
    }

    RtreeRnPage() {}

    RtreeRnPage insert(Storage storage, RectangleRn r, Object obj, int level) {
        modify();
        if (--level != 0) {
            // not leaf page
            int i, mini = 0;
            double minIncr = Double.MAX_VALUE;
            double minArea = Double.MAX_VALUE;
            for (i = 0; i < n; i++) {
                double area = b[i].area();
                double incr = RectangleRn.joinArea(b[i], r) - area;
                if (incr < minIncr) {
                    minIncr = incr;
                    minArea = area;
                    mini = i;
                } else if (incr == minIncr && area < minArea) {
                    minArea = area;
                    mini = i;
                }
            }
            RtreeRnPage p = (RtreeRnPage)branch.get(mini);
            RtreeRnPage q = p.insert(storage, r, obj, level);
            if (q == null) {
                // child was not split
                b[mini].join(r);
                return null;
            } else {
                // child was split
                setBranch(mini, p.cover(),  p);
                return addBranch(storage, q.cover(), q);
            }
        } else {
            return addBranch(storage, new RectangleRn(r), obj);
        }
    }

    int remove(RectangleRn r, Object obj, int level, ArrayList reinsertList) {
        if (--level != 0) {
            for (int i = 0; i < n; i++) {
                if (r.intersects(b[i])) {
                    RtreeRnPage pg = (RtreeRnPage)branch.get(i);
                    int reinsertLevel = pg.remove(r, obj, level, reinsertList);
                    if (reinsertLevel >= 0) {
                        if (pg.n >= card/2) {
                            setBranch(i, pg.cover(), pg);
                            modify();
                        } else {
                            // not enough entries in child
                            reinsertList.add(pg);
                            reinsertLevel = level - 1;
                            removeBranch(i);
                        }
                        return reinsertLevel;
                    }
                }
            }
        } else {
            for (int i = 0; i < n; i++) {
                if (branch.containsElement(i, obj)) {
                    removeBranch(i);
                    return 0;
                }
            }
        }
        return -1;
    }


    void find(RectangleRn r, ArrayList result, int level) {
        if (--level != 0) { /* this is an internal node in the tree */
            for (int i = 0; i < n; i++) {
                if (r.intersects(b[i])) {
                    ((RtreeRnPage)branch.get(i)).find(r, result, level);
                }
            }
        } else { /* this is a leaf node */
            for (int i = 0; i < n; i++) {
                if (r.intersects(b[i])) {
                    result.add(branch.get(i));
                }
            }
        }
    }

    void purge(int level) {
        if (--level != 0) { /* this is an internal node in the tree */
            for (int i = 0; i < n; i++) {
                ((RtreeRnPage)branch.get(i)).purge(level);
            }
        }
        deallocate();
    }

    final void setBranch(int i, RectangleRn r, Object obj) {
        b[i] = r;
        branch.setObject(i, obj);
    }

    final void removeBranch(int i) {
        n -= 1;
        System.arraycopy(b, i+1, b, i, n-i);
        branch.removeObject(i);
        branch.setSize(card);
        modify();
    }

    final RtreeRnPage addBranch(Storage storage, RectangleRn r, Object obj) {
        if (n < card) {
            setBranch(n++, r, obj);
            return null;
        } else {
            return splitPage(storage, r, obj);
        }
    }

    final RtreeRnPage splitPage(Storage storage, RectangleRn r, Object obj) {
        int i, j, seed0 = 0, seed1 = 0;
        double[] rectArea = new double[card+1];
        double   waste;
        double   worstWaste = Double.NEGATIVE_INFINITY;
        //
        // As the seeds for the two groups, find two rectangles which waste
        // the most area if covered by a single rectangle.
        //
        rectArea[0] = r.area();
        for (i = 0; i < card; i++) {
            rectArea[i+1] = b[i].area();
        }
        RectangleRn bp = r;
        for (i = 0; i < card; i++) {
            for (j = i+1; j <= card; j++) {
                waste = RectangleRn.joinArea(bp, b[j-1]) - rectArea[i] - rectArea[j];
                if (waste > worstWaste) {
                    worstWaste = waste;
                    seed0 = i;
                    seed1 = j;
                }
            }
            bp = b[i];
        }
        byte[] taken = new byte[card];
        RectangleRn group0, group1;
        double      groupArea0, groupArea1;
        int         groupCard0, groupCard1;
        RtreeRnPage pg;

        taken[seed1-1] = 2;
        group1 = new RectangleRn(b[seed1-1]);

        if (seed0 == 0) {
            group0 = new RectangleRn(r);
            pg = new RtreeRnPage(storage, obj, r);
        } else {
            group0 = new RectangleRn(b[seed0-1]);
            pg = new RtreeRnPage(storage, branch.getRaw(seed0-1), group0);
            setBranch(seed0-1, r, obj);
        }
        groupCard0 = groupCard1 = 1;
        groupArea0 = rectArea[seed0];
        groupArea1 = rectArea[seed1];
        //
        // Split remaining rectangles between two groups.
        // The one chosen is the one with the greatest difference in area
        // expansion depending on which group - the rect most strongly
        // attracted to one group and repelled from the other.
        //
        while (groupCard0 + groupCard1 < card + 1
               && groupCard0 < card + 1 - card/2
               && groupCard1 < card + 1 - card/2)
        {
            int betterGroup = -1, chosen = -1;
            double biggestDiff = -1;
            for (i = 0; i < card; i++) {
                if (taken[i] == 0) {
                    double diff = (RectangleRn.joinArea(group0, b[i]) - groupArea0)
                              - (RectangleRn.joinArea(group1, b[i]) - groupArea1);
                    if (diff > biggestDiff || -diff > biggestDiff) {
                        chosen = i;
                        if (diff < 0) {
                            betterGroup = 0;
                            biggestDiff = -diff;
                        } else {
                            betterGroup = 1;
                            biggestDiff = diff;
                        }
                    }
                }
            }
            Assert.that(chosen >= 0);
            if (betterGroup == 0) {
                group0.join(b[chosen]);
                groupArea0 = group0.area();
                taken[chosen] = 1;
                pg.setBranch(groupCard0++, b[chosen], branch.getRaw(chosen));
            } else {
                groupCard1 += 1;
                group1.join(b[chosen]);
                groupArea1 = group1.area();
                taken[chosen] = 2;
            }
        }
        //
        // If one group gets too full, then remaining rectangle are
        // split between two groups in such way to balance cards of two groups.
        //
        if (groupCard0 + groupCard1 < card + 1) {
            for (i = 0; i < card; i++) {
                if (taken[i] == 0) {
                    if (groupCard0 >= groupCard1) {
                        taken[i] = 2;
                        groupCard1 += 1;
                    } else {
                        taken[i] = 1;
                        pg.setBranch(groupCard0++, b[i], branch.getRaw(i));
                    }
                }
            }
        }
        pg.n = groupCard0;
        n = groupCard1;
        for (i = 0, j = 0; i < groupCard1; j++) {
            if (taken[j] == 2) {
                setBranch(i++, b[j], branch.getRaw(j));
            }
        }
        // truncate rest of link
        branch.setSize(groupCard1);
        branch.setSize(card);
        return pg;
    }

    final RectangleRn cover() {
        RectangleRn r = new RectangleRn(b[0]);
        for (int i = 1; i < n; i++) {
            r.join(b[i]);
        }
        return r;
    }
}
