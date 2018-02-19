package dynamo;


import java.util.Arrays;
import java.util.HashMap;

import java.util.Map;


import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;

import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;

import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;

import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.document.Item;


/**
 * Data accessor object for rides DynamoDB
 */
public class RidesDao {

    private AmazonDynamoDB dbClient;
    private DynamoDB dynamoDB;

    // Needs to deal with the credentials...
    public RidesDao() {
    	
        dbClient = AmazonDynamoDBClientBuilder.standard()
                        .withRegion(Regions.US_EAST_2)
                        .build();
        dynamoDB = new DynamoDB(dbClient);   
    

    }

    
    
    
    
    public void createTable1() {
    	
        dbClient = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_EAST_2)
                .build();
        dynamoDB = new DynamoDB(dbClient);   
        String tableName = "RideTable";

        try {
            System.out.println("Attempting to create table; please wait...");
            Table table = dynamoDB.createTable(tableName,
                Arrays.asList(new KeySchemaElement("year", KeyType.HASH), // Partition
                                                                          // key
                    new KeySchemaElement("title", KeyType.RANGE)), // Sort key
                Arrays.asList(new AttributeDefinition("year", ScalarAttributeType.N),
                    new AttributeDefinition("title", ScalarAttributeType.S)),
                new ProvisionedThroughput(10L, 10L));
            table.waitForActive();
            System.out.println("Success.  Table status: " + table.getDescription().getTableStatus());

        }
        catch (Exception e) {
            System.err.println("Unable to create table: ");
            System.err.println(e.getMessage());
        }
    }
    
    
    
    public void deleteTable1(String tableName) {
        Table table = dynamoDB.getTable(tableName);
        try {
            System.out.println("Issuing DeleteTable request for " + tableName);
            table.delete();

            System.out.println("Waiting for " + tableName + " to be deleted...this may take a while...");

            table.waitForDelete();
        }
        catch (Exception e) {
            System.err.println("DeleteTable request failed for " + tableName);
            System.err.println(e.getMessage());
        }
    }
    
    
    
    
    // Todo: Implement insert, update, read, readAll, delete
    public void insert() {
    	System.out.print("TEST");
    }
    
    
    public void TestInsert(String email, String name, String church) {
    	
    	dbClient = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_EAST_2)
                .build();
    	dynamoDB = new DynamoDB(dbClient);

    		Table table = dynamoDB.getTable("Test");

        final Map<String, Object> infoMap = new HashMap<String, Object>();
        infoMap.put("email", email);
        infoMap.put("church", church);

        try {
            System.out.println("Adding a new item...");
            PutItemOutcome outcome = table
                .putItem(new Item().withPrimaryKey("Email", email, "Name", name).withMap("info", infoMap));

            System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());

        }
        catch (Exception e) {
            System.err.println("Unable to add item: " + email + " " + name);
            System.err.println(e.getMessage());
        }
    }
    
    
    
    public void TestInsert1(int n1, String n2) {
    	
    	dbClient = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_EAST_2)
                .build();
    	dynamoDB = new DynamoDB(dbClient);

    		Table table = dynamoDB.getTable("RideTable");
        int year = n1;
        String title = n2;

        final Map<String, Object> infoMap = new HashMap<String, Object>();
        infoMap.put("plot", "Nothing happens at all.");
        infoMap.put("rating", 0);
        infoMap.put("Working", 0);
        infoMap.put(n2, n1);

        try {
            System.out.println("Adding a new item...");
            PutItemOutcome outcome = table
                .putItem(new Item().withPrimaryKey("year", year, "title", title).withMap("info", infoMap));

            System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());

        }
        catch (Exception e) {
            System.err.println("Unable to add item: " + year + " " + title);
            System.err.println(e.getMessage());
        }
    }
    
    
    
    public void TestRead(String email, String name) {

        Table table = dynamoDB.getTable("Test");


        GetItemSpec spec = new GetItemSpec().withPrimaryKey("Email", email, "Name", name);

        try {
            System.out.println("Attempting to read the item...");
            Item outcome = table.getItem(spec);
            System.out.println("GetItem succeeded: " + outcome);

        }
        catch (Exception e) {
            System.err.println("Unable to read item: " + email + " " + name);
            System.err.println(e.getMessage());
        }

    }
    
    public void TestUpdate(String email, String name, String update) {
        Table table = dynamoDB.getTable("Test");


        UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("Email", email, "Name", name)
            .withUpdateExpression("set info.email = :e, info.church=:c")
            .withValueMap(new ValueMap().withString(":e", email).withString(":c", update)
                )
            .withReturnValues(ReturnValue.UPDATED_NEW);

        try {
            System.out.println("Updating the item...");
            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
            System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());

        }
        catch (Exception e) {
            System.err.println("Unable to update item: " + email + " " + name);
            System.err.println(e.getMessage());
        }
    }
    
    public void TestRead() {

    	dbClient = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_EAST_2)
                .build();
    	dynamoDB = new DynamoDB(dbClient);
    	

        ScanRequest scanRequest = new ScanRequest()
        	    .withTableName("Test");

        	ScanResult result = dbClient.scan(scanRequest);
        	for (Map<String, AttributeValue> item : result.getItems()){
        	    System.out.print(item);
    }
   
    }
    /**
     * ACTUAL FUNCTIONS FOR THE AACF 
     */
    public void createTable() {
        dbClient = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_WEST_1)
                .build();
dynamoDB = new DynamoDB(dbClient);   
    	/*
        dbClient = AmazonDynamoDBClientBuilder.standard()   // Client
                .withRegion(Regions.US_EAST_2)
                .build();
        dynamoDB = new DynamoDB(dbClient);   
        */
        String tableName = "RideTable2"; //Name for Table

        try {
            System.out.println("Attempting to create table; please wait...");
            Table table = dynamoDB.createTable(tableName,
                Arrays.asList(new KeySchemaElement("Email", KeyType.HASH), // Partition
                                                                          // key
                    new KeySchemaElement("Name", KeyType.RANGE)), // Sort key
                Arrays.asList(new AttributeDefinition("Email", ScalarAttributeType.S),
                    new AttributeDefinition("Name", ScalarAttributeType.S)),
                new ProvisionedThroughput(10L, 10L));
            table.waitForActive();
            System.out.println("Success.  Table status: " + table.getDescription().getTableStatus());

        }
        catch (Exception e) {
            System.err.println("Unable to create table: ");
            System.err.println(e.getMessage());
        }
    }
    
    
    //Delete Table
    public void deleteTable(String tableName) {
        Table table = dynamoDB.getTable(tableName);
        try {
            System.out.println("Issuing DeleteTable request for " + tableName);
            table.delete();

            System.out.println("Waiting for " + tableName + " to be deleted...this may take a while...");

            table.waitForDelete();
        }
        catch (Exception e) {
            System.err.println("DeleteTable request failed for " + tableName);
            System.err.println(e.getMessage());
        }
    }
	public void insert(String email, String name, int year, String phoneNumber, String church, boolean attendance) {
		dbClient = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_1).build();
		dynamoDB = new DynamoDB(dbClient);
		/*
		 * Ryan's Region
		 * 
		 * dbClient = AmazonDynamoDBClientBuilder.standard()
		 * .withRegion(Regions.US_EAST_2) .build(); dynamoDB = new DynamoDB(dbClient);
		 */

		Table table = dynamoDB.getTable("RideTable");

		final Map<String, Object> infoMap = new HashMap<String, Object>();
		infoMap.put("Email", email);
		infoMap.put("Name", name);
		infoMap.put("Church", church);
		infoMap.put("year", year);
		infoMap.put("Phone Number", phoneNumber);
		if (attendance) { // attends or not
			infoMap.put("attendance", "yes");
		} else {
			infoMap.put("attendance", "no");
		}

		try {
			System.out.println("Adding a new item...");
			PutItemOutcome outcome = table
					.putItem(new Item().withPrimaryKey("Email", email, "Name", name).withMap("info", infoMap));

			System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());

		} catch (Exception e) {
			System.err.println("Unable to add item: " + email + " " + name);
			System.err.println(e.getMessage());
		}
	}

	public void update(String newEmail, String name, String oldEmail) {
		Table table = dynamoDB.getTable("RideTable");

		UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("Email", oldEmail, "Name", name)
				.withUpdateExpression("set info.Email = :e").withValueMap(new ValueMap().withString(":e", newEmail))
				.withReturnValues(ReturnValue.UPDATED_NEW);

		try {
			System.out.println("Updating the item...");
			UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
			System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());

		} catch (Exception e) {
			System.err.println("Unable to update item: " + oldEmail + " with the new email: " + newEmail);
			System.err.println(e.getMessage());
		}
	}

    public void read(String email)
    {
    }

    public void readAll()
    {
    }

    public void delete(String email)
    {
    }
}