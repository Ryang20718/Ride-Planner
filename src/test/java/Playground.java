

import dynamo.RidesDao;


/**
 * For testing random things
 */
public class Playground {

    public static void main(String[] args) {

        RidesDao dao = new RidesDao();

        dao.insert();
        dao.TestFunc(10, "Lighthouse");
        System.out.println("hello");
    }
}
