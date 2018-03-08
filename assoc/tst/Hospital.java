import org.garret.perst.*;
import org.garret.perst.assoc.*;

public class Hospital
{
    void populateDatabase()
    {
        ReadWriteTransaction t = db.startReadWriteTransaction();

        Item patient = t.createItem();
        t.link(patient, "class", "patient");
        t.link(patient, "name", "John Smith");
        t.link(patient, "age", 55);
        t.link(patient, "wight", 65.7);
        t.link(patient, "sex", "male");
        t.link(patient, "phone", "1234567");
        t.link(patient, "address", "123456, CA, Dummyngton, Outlook drive, 17");

        Item doctor = t.createItem();
        t.link(doctor, "class", "doctor");
        t.link(doctor, "name", "Robby Wood");
        t.link(doctor, "speciality", "therapeutist");

        t.link(doctor, "patient", patient);

        Item angina = t.createItem();
        t.link(angina, "class", "disease");
        t.link(angina, "name", "angina");
        t.link(angina, "symptoms", "throat ache");
        t.link(angina, "symptoms", "high temperature");
        t.link(angina, "treatment", "milk&honey");
        
        Item flu = t.createItem();
        t.link(flu, "class", "disease");
        t.link(flu, "name", "flu");
        t.link(flu, "symptoms", "stomachache");
        t.link(flu, "symptoms", "high temperature");
        t.link(flu, "treatment", "theraflu");
        
        Item diagnosis = t.createItem();
        t.link(diagnosis, "class", "diagnosis");
        t.link(diagnosis, "disease", flu);
        t.link(diagnosis, "symptoms", "high temperature");
        t.link(diagnosis, "diagnosed-by", doctor);
        t.link(diagnosis, "date", "2010-09-23");
        t.link(patient, "diagnosis", diagnosis);
        
        t.commit();
    }
    
    void searchDatabase() 
    {
        ReadOnlyTransaction t = db.startReadWriteTransaction();

        // Find all patients with age > 50 which are diagnosed flu in last September
        for (Item patient : t.find(Predicate.and(Predicate.compare("age", Predicate.Compare.Operation.GreaterThan, 50),
                                                 Predicate.in("diagnosis", 
                                                              Predicate.and(Predicate.between("date", "2010-09-01", "2010-09-30"),
                                                                            Predicate.in("disease", 
                                                                                         Predicate.compare("name", Predicate.Compare.Operation.Equals, "flu")))))))
        {
            System.out.println("Patient " + patient.getString("name") + ", age " +  patient.getNumber("age"));
        }

        // Print list of diseases with high temperature symptom ordered by name
        for (Item disease : t.find(Predicate.and(Predicate.compare("class", Predicate.Compare.Operation.Equals, "disease"),
                                                 Predicate.compare("symptoms", Predicate.Compare.Operation.Equals, "high temperature")), 
                                   new OrderBy("name")))
        {
            System.out.println("Diseas " + disease.getString("name"));
            Object symptoms = disease.getAttribute("symptoms");
            if (symptoms instanceof String) { 
                System.out.println("Symptom: " + symptoms);
            } else if (symptoms instanceof String[]) { 
                System.out.println("Symptoms: ");
                String[] ss = (String[])symptoms;
                for (int i = 0; i < ss.length; i++) {
                    System.out.println(Integer.toString(i+1) + ": " + ss[i]);
                }
            }
        }
        t.commit();
    }

    void updateDatabase()
    {
        ReadWriteTransaction t = db.startReadWriteTransaction();
        Item patient = t.find(Predicate.and(Predicate.compare("class", Predicate.Compare.Operation.Equals, "patient"),
                                            Predicate.compare("name", Predicate.Compare.Operation.Equals, "John Smith"))).first();
        t.update(patient, "age", 56);
        t.commit();
    }

    void shutdown()
    {
        storage.close();
    }

    Hospital()
    {
        storage = StorageFactory.getInstance().createStorage();
        storage.open("hospital.dbs");
        db = new AssocDB(storage);
    }
     
    public static void main(String[] args) 
    {
        Hospital hospital = new Hospital();
        hospital.populateDatabase();
        hospital.searchDatabase();
        hospital.updateDatabase();
        hospital.shutdown();
    }
   
    AssocDB db;
    Storage storage;
}