package com.atoz.aws.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Value;

import software.amazon.awssdk.services.dynamodb.model.DynamoDBException;
import software.amazon.awssdk.services.dynamodb.DynamoDBClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.HashMap;
import java.util.Map;

@Service
public class DynamoDbAccessService {
    private static final Logger log = LoggerFactory.getLogger(DynamoDbAccessService.class);

    @Value("${aws.dynamoDb.table.name}")
    private String tableName;

    @Value("${aws.dynamoDb.table.key}")
    private String keyName;

    @Value("${aws.dynamoDb.table.attribute.fullname}")
    private String attrFullName;

    private DynamoDBClient dbClient;

    public DynamoDbAccessService() {
        dbClient = DynamoDBClient.create();
    }

    public DynamoDbAccessService(String tableName, String keyName) {
        this.tableName = tableName;
        this.keyName = keyName;
        this.dbClient = DynamoDBClient.create();
    }

    public void putItem(String keyValue, Map<String, AttributeValue> extraAttributes) throws Exception {
        HashMap<String, AttributeValue> item_values = new HashMap<>();
        item_values.put(keyName, AttributeValue.builder().s(keyValue).build());

        item_values.putAll(extraAttributes);
        PutItemRequest request = PutItemRequest.builder()
                .tableName(tableName)
                .item(item_values)
                .build();

        try {
            dbClient.putItem(request);
        } catch (ResourceNotFoundException rnfe) {
            log.error("Error put item into table {}: {}", tableName, rnfe.getMessage());
            throw rnfe;
        } catch (DynamoDBException dbe) {
            log.error("Error to put item into table {}: {}", tableName, dbe.getMessage());
            throw dbe;
        }
    }

    public Map<String, AttributeValue> getItem(String key) throws DynamoDBException {
        HashMap<String,AttributeValue> key_to_get = new HashMap<>();
        log.info("Search DynamoDB with key={}, and tableName={}", keyName, tableName);
        key_to_get.put(keyName, AttributeValue.builder()
                .s(key).build());

        GetItemRequest request = GetItemRequest.builder()
                    .key(key_to_get)
                    .tableName(tableName)
                    .build();

        try {
            log.info("calling DynamoDb");
            GetItemResponse response = dbClient.getItem(request);
            if (response.item() == null) {
                log.info("Item not found {}", key);
            } else {
                log.info("return from DynamoDb call with item size={}", response.item().size());
            }
            return response.item();
        } catch (DynamoDBException e) {
            log.error("Error to get item from table {}: {}", tableName, e.getErrorMessage());
            throw e;
        }
    }

    public void deleteItem(String key) {
        HashMap<String,AttributeValue> keyMap = new HashMap<>();
        keyMap.put(keyName, AttributeValue.builder().s(key).build());
        DeleteItemRequest request = DeleteItemRequest.builder()
                .key(keyMap)
                .tableName(tableName)
                .build();

        try {
            DeleteItemResponse response = dbClient.deleteItem(request);
        } catch (DynamoDBException e) {
            log.error("Error to delete item {} from table {}", key, tableName);
        }
    }

    public static String deleteTable(String tableName) {
        DeleteTableRequest request = DeleteTableRequest.builder().tableName(tableName).build();

        DeleteTableResponse response = DynamoDBClient.create().deleteTable(request);
        return response.tableDescription().tableName();
    }

    public static boolean tableExists(String tableName) {
        ListTablesRequest request = ListTablesRequest.builder().build();
        ListTablesResponse response = DynamoDBClient.create().listTables(request);

        return response.tableNames().contains(tableName);
    }

    public static String createImageCollectionTable(String tableName, String key)
            throws DynamoDBException {

        if (tableExists(tableName)) {
            log.error("Table with name {} already exists!", tableName);
            throw new DynamoDBException(tableName + " table already exists!");
        }

        CreateTableRequest request = CreateTableRequest.builder()
                .keySchema(KeySchemaElement.builder()
                        .attributeName(key)
                        .keyType(KeyType.HASH)
                        .build())
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName(key)
                        .attributeType(ScalarAttributeType.S)
                        .build())
                .provisionedThroughput(ProvisionedThroughput.builder()
                        .readCapacityUnits(new Long(10))
                        .writeCapacityUnits(new Long(10))
                        .build())
                .tableName(tableName)
                .build();

        CreateTableResponse response = DynamoDBClient.create().createTable(request);

        return response.tableDescription().tableName();
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getKeyName() {
        return keyName;
    }

    public void setKeyName(String keyName) {
        this.keyName = keyName;
    }

    public String getAttrFullName() { return attrFullName; }

    public void setAttrFullName(String attrFullName) {
        this.attrFullName = attrFullName;
    }

    public DynamoDBClient getDbClient() {
        return dbClient;
    }

    public void setDbClient(DynamoDBClient dbClient) {
        this.dbClient = dbClient;
    }
}
