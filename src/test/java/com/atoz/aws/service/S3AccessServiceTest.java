package com.atoz.aws.service;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest
public class S3AccessServiceTest {

    @Autowired
    private S3AccessService service;

    private String imageFile = "Xifeng2.jpg.2018-06-18-212024";

    @Test
    public void testFileUpload() throws Exception {
        if (service.isObjectExists(imageFile)) {
            service.deleteFile(imageFile);
        }

        service.uploadFile(imageFile,
                getImageFilePath(imageFile),
                getMetaData("fullname", "Austin Zhang"));

        assert service.isObjectExists(imageFile);
    }

    @Test
    public void testDownLoad() throws Exception {
        String filePath = getDownloadFileNameWithTimestamp(imageFile);
        if (service.isObjectExists(imageFile)) {
            service.downLoadFile(imageFile, filePath);
        }

        File file = new File(filePath);
        assert file.exists();
    }

    private String getImageFilePath(String fileName) throws IOException {
        return new ClassPathResource("./images/" + fileName).getFile().getAbsolutePath();
    }

    private Map<String, String> getMetaData(String key, String value) {
        Map<String, String> map = new HashMap<>();
        map.put(key, value);

        return map;
    }

    private String getDownloadFileNameWithTimestamp(String key) throws IOException {
        String timestamp = new SimpleDateFormat("yyyy-MM-dd-HHmmss").format(new Date());
        return new ClassPathResource("./images/").getFile().getAbsolutePath() + "/" + key + "." + timestamp;
    }
}