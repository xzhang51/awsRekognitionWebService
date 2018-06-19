package com.atoz.aws.service;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static org.junit.Assert.*;

@RunWith(SpringRunner.class)
public class AtoZImageRekognitionServiceTest {
    AtoZImageRekognitionService service = new AtoZImageRekognitionService("family_collection");
    DynamoDbAccessService dbService = new DynamoDbAccessService("image_collection", "face_id");

    @Before
    public void setup() {
        dbService.setAttrFullName("fullname");
        service.setDynamoService(dbService);
    }

    @Test
    public void testImageIndexWithFile() throws Exception {
        service.imageIndex(getImageFile("Indexed.jpg"), "Austin & Xifeng");
    }

    @Test
    public void testImageIndexWithStream() throws Exception {
        InputStream stream = new FileInputStream(getImageFile("Austin1.jpg"));
        service.imageIndex(stream, "Austin Zhang");
    }

    @Test
    public void testImageMatch() throws Exception {
        Map<String, Float> result = service.matchImage(getImageFile("Austin2.jpg"));

        assert result.containsKey("Austin Zhang");
        System.out.println(result.toString());
    }

    private File getImageFile(String fileName) throws IOException {
        return new ClassPathResource("./images/" + fileName).getFile();
    }
}