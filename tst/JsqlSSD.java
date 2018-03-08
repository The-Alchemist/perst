// Supplier - Shipment - Detail example
// Implementation using JSQL and Database class

import org.garret.perst.*;
import java.util.*;
import java.io.*;

public class JsqlSSD {
    static class Supplier {
        String   name;
        String   location;
        Link     shipments;
        boolean  partner;
    }
    
    static class Detail {
        String   id;
        float    weight;
        Link     shipments;
    }
    
    static class Shipment { 
        Supplier supplier;
        Detail   detail;
        int      quantity;
        long     price;
    }


    static byte[] inputBuffer = new byte[256];

    static void skip(String prompt) {
        try { 
            System.out.print(prompt);
            System.in.read(inputBuffer);
        } catch (IOException x) {}
    }

    static String input(String prompt) {
        while (true) { 
            try { 
                System.out.print(prompt);
                int len = System.in.read(inputBuffer);
                String answer = new String(inputBuffer, 0, len).trim();
                if (answer.length() != 0) {
                    return answer;
                }
            } catch (IOException x) {}
        }
    }

    static long inputLong(String prompt) { 
        while (true) { 
            try { 
                return Long.parseLong(input(prompt), 10);
            } catch (NumberFormatException x) { 
                System.err.println("Invalid integer constant");
            }
        }
    }

    static double inputDouble(String prompt) { 
        while (true) { 
            try { 
                return Double.parseDouble(input(prompt));
            } catch (NumberFormatException x) { 
                System.err.println("Invalid floating point constant");
            }
        }
    }

    static public void main(String[] args) {    
        Storage storage = StorageFactory.getInstance().createStorage();
        Supplier   supplier;
        Detail     detail;
        Shipment   shipment;
        Iterator   iterator;
        Set        result;
        int        i;

        storage.open("testssd2.dbs");
        Database db = new Database(storage);

        db.createTable(Supplier.class);
        db.createIndex(Supplier.class, "name", true);
        db.createTable(Detail.class);
        db.createIndex(Detail.class, "id", true);
        db.createTable(Shipment.class);

        Query supplierQuery = db.prepare(Supplier.class, "name like ?");
        Query detailQuery = db.prepare(Detail.class, "id like ?");

        while (true) { 
            try { 
                switch ((int)inputLong("-------------------------------------\n" + 
                                       "Menu:\n" + 
                                       "1. Add supplier\n" + 
                                       "2. Add detail\n" + 
                                       "3. Add shipment\n" + 
                                       "4. List of suppliers\n" + 
                                       "5. List of details\n" + 
                                       "6. List of partners\n" + 
                                       "7. Suppliers of detail\n" + 
                                       "8. Details shipped by supplier\n" + 
                                       "9. Exit\n\n>>"))
                {
                  case 1:
                    supplier = new Supplier();
                    supplier.name = input("Supplier name: ");
                    supplier.location = input("Supplier location: ");
                    supplier.shipments = storage.createLink();
                    supplier.partner = input("Is partner (y/n): ").startsWith("y");
                    db.addRecord(supplier);
                    storage.commit();
                    continue;
                  case 2:
                    detail = new Detail();
                    detail.id = input("Detail id: ");
                    detail.weight = (float)inputDouble("Detail weight: ");
                    detail.shipments = storage.createLink();
                    db.addRecord(detail);
                    storage.commit();
                    continue;
                  case 3:
                    iterator = db.select(Supplier.class, "name='" + 
                                         input("Supplier name: ") + "'");
                    if (!iterator.hasNext()) { 
                        System.err.println("No such supplier!");
                        break;
                    }
                    supplier = (Supplier)iterator.next();
                    iterator = db.select(Detail.class, "id='" + 
                                         input("Detail ID: ") + "'");
                    if (!iterator.hasNext()) { 
                        System.err.println("No such detail!");
                        break;
                    }
                    detail = (Detail)iterator.next();
                    shipment = new Shipment();
                    shipment.quantity = (int)inputLong("Shipment quantity: ");
                    shipment.price = inputLong("Shipment price: ");
                    shipment.detail = detail;
                    shipment.supplier = supplier;
                    db.addRecord(shipment);
                    detail.shipments.add(shipment);
                    supplier.shipments.add(shipment);
                    storage.commit();
                    continue;
                  case 4:
                    iterator = db.getRecords(Supplier.class);
                    while (iterator.hasNext()) { 
                        supplier = (Supplier)iterator.next();
                        System.out.println("Supplier name: " + supplier.name + ", supplier.location: " + supplier.location + ", supplier.partner=" + supplier.partner);
                    }
                    break;
                  case 5:
                    iterator = db.getRecords(Detail.class);
                    while (iterator.hasNext()) { 
                        detail = (Detail)iterator.next();
                        System.out.println("Detail ID: " + detail.id + ", detail.weight: " + detail.weight);
                    }
                    break;
                  case 6:
                    iterator = db.select(Supplier.class, "partner = true");
                    while (iterator.hasNext()) { 
                        supplier = (Supplier)iterator.next();
                        System.out.println("Supplier name: " + supplier.name + ", supplier.location: " + supplier.location);
                    }
                    break;
                  case 7:
                    detailQuery.setParameter(1, input("Detail ID: "));
                    iterator = detailQuery.execute();
                    result = new HashSet();
                    while (iterator.hasNext()) {                         
                        detail = (Detail)iterator.next();
                        for (i = detail.shipments.size(); --i >= 0;) { 
                            shipment = (Shipment)detail.shipments.get(i);
                            result.add(shipment.supplier);
                        }
                    }
                    iterator = result.iterator();
                    while (iterator.hasNext()) { 
                        supplier = (Supplier)iterator.next();
                        System.out.println("Suppplier name: " + supplier.name);
                    }
                    break;
                  case 8:
                    supplierQuery.setParameter(1, input("Supplier name: "));
                    iterator = supplierQuery.execute();
                    result = new HashSet();
                    while (iterator.hasNext()) {                         
                        supplier = (Supplier)iterator.next();
                        for (i = supplier.shipments.size(); --i >= 0;) { 
                            shipment = (Shipment)supplier.shipments.get(i);
                            result.add(shipment.detail);
                        }
                    }
                    iterator = result.iterator();
                    while (iterator.hasNext()) { 
                        detail = (Detail)iterator.next();
                        System.out.println("Detial ID: " + detail.id);
                    }
                    break;
                  case 9:
                    storage.close();
                    return;
                }
                skip("Press ENTER to continue...");
            } catch (StorageError x) { 
                System.out.println("Error: " + x.getMessage());
                skip("Press ENTER to continue...");
            }
        }
    }
}
