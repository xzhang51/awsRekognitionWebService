package com.atoz.aws.service;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

@RunWith(SpringRunner.class)
@SpringBootTest
public class DynamoDbAccessServiceTest {
    private String tableName = "image_collection";
    private String keyName = "face_id";
    private String attrFullname = "fullname";
    private DynamoDbAccessService service = new DynamoDbAccessService(tableName, keyName);

    private String keyValue = "deb84eb1-f679-4960-9ba3-824805480a21";
    private String fullNameValue = "Austin& Xifeng";

    public void testCreateTable() throws Exception {
        if (!DynamoDbAccessService.tableExists(tableName)) {
            List<String> attrs = new ArrayList<>();
            attrs.add(attrFullname);

            assertEquals(tableName, DynamoDbAccessService.createImageCollectionTable(tableName, keyName));
            Thread.sleep(5000);
        } else {
            System.out.println("Table already created!");
        }

        assert DynamoDbAccessService.tableExists(tableName);
    }

    @Test
    public void testPutItem() throws Exception {
        Map<String, AttributeValue> extraAttrs = new HashMap<>();
        extraAttrs.put(attrFullname, AttributeValue.builder().s(fullNameValue).build());

        service.putItem(keyValue, extraAttrs);

        Map<String, AttributeValue> resultMap = service.getItem(keyValue);
        assertEquals(fullNameValue, resultMap.get(attrFullname).s());
    }

    @Test
    public void testGetItem() throws Exception {
        Map<String, AttributeValue> result = service.getItem(keyValue);

        assert !result.isEmpty();
        assertEquals(fullNameValue, result.get(attrFullname).s());
    }

    @Test
    public void testDeleteItem() throws Exception {
        service.deleteItem(keyValue);
    }

//    @Test
//    public void testDeleteTable() throws Exception {
//        DynamoDbAccessService.deleteTable(tableName);
//    }
}