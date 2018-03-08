import java.io.*;

import org.garret.perst.*;
import org.garret.perst.continuous.*;

class Address implements IValue 
{ 
    @FullTextSearchable
    private String country;

    @FullTextSearchable
    private String city;

    @FullTextSearchable
    private String street;

    public String getCountry() { 
        return country;
    }

    public String getCity() { 
        return city;
    }

    public String getStreet() { 
        return street;
    }

    public String toString() { 
        return "country=" + country + ", city=" + city + ", street=" + street;
    }


    public Address(String country, String city,  String street) { 
        this.country = country;
        this.city = city;
        this.street = street;
    }
    
    private Address() {}
};


class Company extends CVersion
{
    @FullTextSearchable
    @Indexable(unique=true)
    private String name;

    @FullTextSearchable
    private Address location;    

    public String getName() { 
        return name;
    }

    public Address getLocation() { 
        return location;
    }

    public IterableIterator<Employee> getEmployees() { 
        return CDatabase.instance.find(Employee.class, "company", new Key(this));
    }

    public String toString() { 
        return "name='" + name + "', " + location;
    }

    public Company(String name, Address location) { 
        this.name = name;                      
        this.location = location;
    }

    private Company() {}
}

class Employee extends CVersion
{ 
    @FullTextSearchable
    @Indexable(unique=true)
    private String name;

    private int age;

    @Indexable
    private CVersionHistory<Company> company;

    public String getName() { 
        return name;
    }

    public int getAge() { 
        return age;
    }

    public Company getCompany() { 
        return company.getCurrent();
    }

    public void setCompany(Company company) { 
        this.company = company.getVersionHistory();
    }

    public Employee(String name, int age, Company company) { 
        this.name = name;
        this.age = age;
        setCompany(company);
    }

    public String toString() { 
        return "name='" + getName() + "', age=" + getAge() + ", company=" + getCompany().getName();
    }

    private Employee() {}
}


public class SimpleRelation 
{ 
    static byte[] inputBuffer = new byte[256];

    static String input(String prompt) 
    {
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

    static long inputNumber(String prompt) 
    {
        while (true) { 
            String s = input(prompt);
            try { 
                return Long.parseLong(s);
            } catch (NumberFormatException x) { 
                System.out.println("Bad number format");
            }
        }
    }

    public static void main(String[] args) 
    {
        Storage storage = StorageFactory.getInstance().createStorage();
        storage.open("simple.dbs");
        CDatabase db = CDatabase.instance;
        db.open(storage, "index");

        System.out.println("Example of Continuous application: Company-Employee database");

        while (true) { 
            System.out.println("Menu:");
            System.out.println("1. New company");
            System.out.println("2. New employee");
            System.out.println("3. Find company");
            System.out.println("4. Find employee");
            System.out.println("5. Print employees");
            System.out.println("6. Full test search");            
            System.out.println("7. Set snapshot");
            System.out.println("8. Exit");
            
            try { 
                switch ((int)inputNumber("> ")) { 
                case 1:
                {
                    db.beginTransaction();
                    String name    = input("Company name: ");
                    String country = input("Country: ");
                    String city    = input("City: ");
                    String street  = input("Street: ");
                    Company company = new Company(name, new Address(country, city, street));
                    db.insert(company);
                    db.commitTransaction();
                    break;
                }
                case 2:
                {
                    db.beginTransaction();
                    String name  = input("Employee name: ");
                    int age = (int)inputNumber("Age: ");
                    String companyName = input("Employer name: ");
                    Company company = db.getSingleton(db.<Company>find(Company.class, "name", new Key(companyName)));
                    if (company == null) { 
                        System.out.println("No such company " + companyName);
                    } else { 
                        Employee employee = new Employee(name, age, company);
                        db.insert(employee);
                        db.commitTransaction();
                    }
                    break;
                }
                case 3:
                {
                    String query = input("Query to select companies: ");
                    for (Company c : db.<Company>select(Company.class, query)) { 
                        System.out.println(c);
                    }
                    break;
                }
                case 4:
                {
                    String query = input("Query to select employees: ");
                    for (Employee e : db.<Employee>select(Employee.class, query)) { 
                        System.out.println(e);
                    }
                    break;
                }
                case 5:
                {
                    String companyName = input("Company name: ");
                    Company company = db.getSingleton(db.<Company>find(Company.class, "name", new Key(companyName)));
                    if (company == null) { 
                        System.out.println("No such company " + companyName);
                    } else { 
                        for (Employee e : company.getEmployees()) { 
                            System.out.println(e.getName());
                        }
                    }
                    break;
                } 
                case 6:
                {
                    String query = input("Full text search query: ");
                    FullTextSearchResult[] results = db.fullTextSearch(query, Integer.MAX_VALUE);
                    for (int i = 0; i < results.length; i++) { 
                        System.out.println(Integer.toString(i+1) + ':' + results[i].getScore() + '\t' + results[i].getVersion());
                    }
                    break;
                }
                case 7:
                {
                    long transId = inputNumber("Transaction ID: ");
                    db.beginTransaction(transId);
                    break;
                }
                case 8:
                    db.close();
                    return;
                }
            } catch (Exception x) { 
                x.printStackTrace();
            }
            System.out.println("-----------------------");
        }
    }
}