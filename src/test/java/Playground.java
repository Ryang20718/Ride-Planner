

import dynamo.RidesDao;


/**
 * For testing random things
 */
public class Playground {

    public static void main(String[] args) {

        RidesDao dao = new RidesDao();
        dao.insert();
        dao.TestInsert("test@gmail.com", "test", "CCAC");
        dao.TestRead("test@gmail.com", "test");
        dao.TestUpdate("test@gmail.com", "test", "Lighthouse");
        //dao.TestRead("ryan@gmail.com", "Kevin");
        System.out.println("hello");
    }
}
