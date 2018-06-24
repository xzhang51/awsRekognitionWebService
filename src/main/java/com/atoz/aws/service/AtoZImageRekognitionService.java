package com.atoz.aws.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.*;
import com.amazonaws.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@Service
public class AtoZImageRekognitionService {
    private static final Logger log = LoggerFactory.getLogger(AtoZImageRekognitionService.class);

    @Autowired
    private DynamoDbAccessService dynamoService;

    @Value("${aws.rekognition.image.collection}")
    private String imageCollection;

    private final AmazonRekognition client;

    public AtoZImageRekognitionService() {
        client = AmazonRekognitionClientBuilder.defaultClient();
    }

    public AtoZImageRekognitionService(String imageCollection) {
        this.imageCollection = imageCollection;
        this.client = AmazonRekognitionClientBuilder.defaultClient();
    }

    public void imageIndex(File file, String name) throws Exception {
        Image image = getImageFromFile(file);

        IndexFacesRequest indexRequest = new IndexFacesRequest().withImage(image).withCollectionId(imageCollection);
        IndexFacesResult indexResults = client.indexFaces(indexRequest);
        List<FaceRecord> faceRecs = indexResults.getFaceRecords();
        if (faceRecs.isEmpty()) {
            log.info("No image indexed from image file: {}", file.getAbsolutePath());
        } else {
            // only index the largest face in the image
            for (FaceRecord face : faceRecs) {
                dynamoService.putItem(face.getFace().getFaceId(),
                        buildExtraDbItemAttributes(dynamoService.getAttrFullName(), name));
            }
            log.info("{} FaceIds are indexed from the imdage file {}", faceRecs.size(), file.getAbsolutePath());
        }
    }

    /**
     * @param inputStream
     * @param name
     * @throws Exception
     */
    public void imageIndex(InputStream inputStream, String name) throws Exception {
        ByteBuffer imageBytes = ByteBuffer.wrap(IOUtils.toByteArray(inputStream));
        Image image = new Image().withBytes(imageBytes);

        IndexFacesRequest indexRequest = new IndexFacesRequest().withImage(image).withCollectionId(imageCollection);
        IndexFacesResult indexResults = client.indexFaces(indexRequest);
        List<FaceRecord> faceRecs = indexResults.getFaceRecords();
        if (faceRecs.isEmpty()) {
            log.info("No image indexed");
        } else {
            for (FaceRecord face : faceRecs) {
                dynamoService.putItem(face.getFace().getFaceId(),
                        buildExtraDbItemAttributes(dynamoService.getAttrFullName(), name));
            }
            log.info("{} Face Ids are indexed for {}", faceRecs.size(), name);
        }
    }

    /**
     * The Image file to be matched has to be on the server file system.
     *
     * @param file Image file path of the application host file system.
     * @return Matched face name and confidence percentage.
     * @throws Exception
     */
    public Map<String, Float> matchImage(File file) throws Exception {
        boolean matched = false;
        SearchFacesByImageRequest searchRequest = new SearchFacesByImageRequest()
                .withCollectionId(imageCollection)
                .withImage(getImageFromFile(file));

        SearchFacesByImageResult searchResult = client.searchFacesByImage(searchRequest);
        Map<String, Float> matchResult = new HashMap<>();
        for (FaceMatch match : searchResult.getFaceMatches()) {
            log.info("Number of faces matched: {}", searchResult.getFaceMatches().size());
            String faceId = match.getFace().getFaceId();

            Map<String, AttributeValue> matchedInfo = dynamoService.getItem(faceId);
            String fullName = matchedInfo.get(dynamoService.getAttrFullName()).s();
            float confidence = match.getFace().getConfidence();
            matchResult.put(fullName, confidence);
        }

        return matchResult;
    }

    public Map<String, Float> matchImage(InputStream inputStream) throws Exception {
        ByteBuffer imageBytes = ByteBuffer.wrap(IOUtils.toByteArray(inputStream));
        Image image = new Image().withBytes(imageBytes);

        SearchFacesByImageRequest searchRequest = new SearchFacesByImageRequest()
                .withCollectionId(imageCollection)
                .withImage(image);

        SearchFacesByImageResult searchResult = client.searchFacesByImage(searchRequest);

        Map<String, Float> matchedFaces = new HashMap<>();

        for (FaceMatch match : searchResult.getFaceMatches()) {
            log.info("Number of faces matched: " + searchResult.getFaceMatches().size());
            String faceId = match.getFace().getFaceId();
            Map<String, AttributeValue> map = dynamoService.getItem(faceId);
            String fullName = map.get(dynamoService.getAttrFullName()).s();
            matchedFaces.put(fullName, match.getFace().getConfidence());
        }

        return matchedFaces;
    }

    public Map<String, Float> detectLabels(InputStream inputStream) throws Exception {
        ByteBuffer imageBytes = ByteBuffer.wrap(IOUtils.toByteArray(inputStream));
        Image image = new Image().withBytes(imageBytes);

        DetectLabelsRequest request = new DetectLabelsRequest()
                .withImage(image)
                .withMaxLabels(100)
                .withMinConfidence(60.0F);

        DetectLabelsResult result = client.detectLabels(request);

        Map<String, Float> matchedLables = new HashMap<>();
        for (Label label : result.getLabels()) {
            matchedLables.put(label.getName(), label.getConfidence());
        }

        return matchedLables;
    }

    public Map<String, Float> detectLablesWithLocalFile(String fileLocaton) throws Exception {
        InputStream inputStream = new FileInputStream(fileLocaton);

        return detectLabels(inputStream);
    }

    public void deleteFaces(List<String> faceIds) {
        DeleteFacesRequest deleteFacesRequest = new DeleteFacesRequest()
                .withCollectionId(imageCollection)
                .withFaceIds(faceIds);

        client.deleteFaces(deleteFacesRequest);
    }

    private String getImageFileName(String imageFilePath) {
        int index = imageFilePath.lastIndexOf("/");
        if (index > 0) {
            return imageFilePath.substring(index + 1);
        } else {
            return imageFilePath;
        }
    }

    private Image getImageFromFile(File file) throws Exception {
        ByteBuffer imageBytes;
        try (InputStream inputStream = new FileInputStream(file)) {
            imageBytes = ByteBuffer.wrap(IOUtils.toByteArray(inputStream));
        }

        return new Image().withBytes(imageBytes);
    }

    private Map<String, AttributeValue> buildExtraDbItemAttributes(String name, String value) {
        Map<String, AttributeValue> map = new HashMap<>();
        map.put(name, AttributeValue.builder().s(value).build());

        return map;
    }

    public static String createCollection(String collectionName) {
        CreateCollectionRequest request = new CreateCollectionRequest().withCollectionId(collectionName);
        CreateCollectionResult result = AmazonRekognitionClientBuilder.defaultClient().createCollection(request);

        return result.getCollectionArn();
    }

    public static void deleteCollection(String collectionName) {
        DeleteCollectionRequest request = new DeleteCollectionRequest().withCollectionId(collectionName);
        AmazonRekognitionClientBuilder.defaultClient().deleteCollection(request);
    }

    public static DetectFacesResult detectFaces (String imageFile) throws Exception {
        ByteBuffer imageBytes;
        try (InputStream inputStream = new FileInputStream(new File(imageFile))) {
            imageBytes = ByteBuffer.wrap(IOUtils.toByteArray(inputStream));
        }

        AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.defaultClient();
        Image image = new Image().withBytes(imageBytes);

        DetectFacesRequest request = new DetectFacesRequest().withImage(image);

        try {
            DetectFacesResult result = rekognitionClient.detectFaces(request);

            for (FaceDetail face : result.getFaceDetails()) {
                log.info(face.toString());
            }

            return result;
        } catch (AmazonRekognitionException e) {
            e.printStackTrace();
        }

        return null;
    }

    public DynamoDbAccessService getDynamoService() {
        return dynamoService;
    }

    public void setDynamoService(DynamoDbAccessService dynamoService) {
        this.dynamoService = dynamoService;
    }

    public String getImageCollection() {
        return imageCollection;
    }

    public void setImageCollection(String imageCollection) {
        this.imageCollection = imageCollection;
    }

    public AmazonRekognition getClient() {
        return client;
    }
}
