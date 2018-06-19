package com.atoz.aws.controller;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
public class AwsRekognitionControllerTest {
    @Autowired
    private MockMvc mvc;

    @Test
    public void upLoadFileToS3() throws Exception {
        MockMultipartFile file = buildMultipartFile("Austin2.jpg");
                mvc.perform(MockMvcRequestBuilders.multipart("/image/s3upload")
                .file(file).param("name", "Austin Zhang #2"))
                .andExpect(status().isOk());
    }

    @Test
    public void testImageIndex() throws Exception {
        MockMultipartFile file = buildMultipartFile("Austin2.jpg");
        mvc.perform(MockMvcRequestBuilders.multipart("/image/index")
                .file(file).param("name", "Austin Zhang #2"))
                .andExpect(status().isOk())
                .andExpect(content().string(equalTo("Image is indexed for Austin Zhang #2")));
    }

    @Test
    public void testImageMatch() throws Exception {
        MockMultipartFile file = buildMultipartFile("Austin2.jpg");
        mvc.perform(MockMvcRequestBuilders.multipart("/image/match")
                .file(file))
                .andExpect(status().isOk())
                .andExpect(content().json("{\"Austin Zhang #2\":100.0,\"Austin & Xifeng\":99.99889,\"Austin Zhang\":100.0}"));
    }

    private MockMultipartFile buildMultipartFile(String fileName) throws IOException {
        return new MockMultipartFile("file", "Austin2.jpg", "image/jpeg",
                new FileInputStream(getImageFile(fileName)));
    }

    private File getImageFile(String fileName) throws IOException {
        return new ClassPathResource("./images/" + fileName).getFile();
    }
}