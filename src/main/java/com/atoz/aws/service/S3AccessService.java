package com.atoz.aws.service;

import com.amazonaws.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.AmazonServiceException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.sync.RequestBody;
import software.amazon.awssdk.sync.StreamingResponseHandler;

import java.io.*;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

@Component
public class S3AccessService {
    private static final Logger log = LoggerFactory.getLogger(S3AccessService.class);

    @Value("${aws.s3.bucket.name}")
    private String bucketName;

    @Value("${aws.s3.bucket.folder}")
    private String folderName;

    private S3Client s3;

    public S3AccessService() {
        s3 = S3Client.create();
    }

    /**
     * Local file upload.
     * @param key
     * @param filePath
     * @param metaData
     */
    public void uploadFile(String key, String filePath, Map<String, String> metaData) {
        File file = new File(filePath);

        try {
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(buildKeyWithFolder(key))
                    .metadata(metaData).build();

            s3.putObject(request, RequestBody.of(file));
        } catch (AmazonServiceException e) {
            log.error("Amazeon service error: {}", e.getErrorMessage());
        }
    }

    /**
     * Web service file upload
     * @param key
     * @param inputStream
     * @param metaData
     * @throws IOException
     * @throws AmazonServiceException
     */
    public void uploadInputStreram(String key, InputStream inputStream, Map<String, String> metaData)
            throws IOException, AmazonServiceException {

        try {
            byte[] bytes = IOUtils.toByteArray(inputStream);

            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(buildKeyWithFolder(key))
                    .metadata(metaData)
                    .build();

            RequestBody body = RequestBody.of(bytes);

            s3.putObject(request, body);
        } catch (IOException ioe) {
            log.error("Input stream cannot read to a byte stream: {}", ioe.getMessage());
            throw ioe;
        } catch (AmazonServiceException e) {
            log.error("Amazeon service error: {}", e.getErrorMessage());
            throw e;
        }
    }

    /**
     * Download file from S3 to local file system.
     * @param key
     * @param destFilePath
     */
    public void downLoadFile(String key, String destFilePath) {
        String keyWithFolder = buildKeyWithFolder(key);
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(keyWithFolder).build();

        String filePath = buildFilePath(destFilePath, key);
        log.info("Download file destination: {}", filePath);
        s3.getObject(request, StreamingResponseHandler.toFile(Paths.get(filePath)));
    }

    /**
     * Download to a output stream
     * @param key
     */
    public byte[] downLoadFileToByteArray(String key) {

        String keyWithFolder = buildKeyWithFolder(key);
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(keyWithFolder).build();

        log.info("Downloading file for key = {}.", keyWithFolder);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        if (isObjectExists(key)) {
            s3.getObject(request, StreamingResponseHandler.toOutputStream(outputStream));
            return outputStream.toByteArray();
        } else {
            log.warn("Image not found");
            return null;
        }
    }


    public void deleteFile(String key) {
        DeleteObjectRequest request = DeleteObjectRequest.builder().bucket(bucketName).key(buildKeyWithFolder(key)).build();
        s3.deleteObject(request);
    }

    public boolean isObjectExists(String key) {
        ListObjectsV2Request listReq = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .maxKeys(100)
                .build();
        ListObjectsV2Response listRes = s3.listObjectsV2(listReq);
        List<S3Object> objects = listRes.contents();

        if (objects.isEmpty()) {
            return false;
        } else {
            return objects.stream().filter(p -> p.key().equals(buildKeyWithFolder(key))).findAny().isPresent();
        }
    }

    private String buildFilePath(String destFilePathOrDir, String key) {
        if (destFilePathOrDir.endsWith("/")) {
            return destFilePathOrDir + key;
        } else {
            return destFilePathOrDir;
        }
    }

    private String buildKeyWithFolder(String key) {
        if (StringUtils.isEmpty(folderName)) {
            return key;
        } else {
            return folderName + "/" + key;
        }
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public S3Client getS3() {
        return s3;
    }

    public void setS3(S3Client s3) {
        this.s3 = s3;
    }

    public String getFolderName() {
        return folderName;
    }

    public void setFolderName(String folderName) {
        this.folderName = folderName;
    }
}
