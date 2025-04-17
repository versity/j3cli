package com.example;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class S3MultipartUploadUtility {

    public static void main(String[] args) throws IOException {
        if (args.length < 7) { // Update the required argument count
            System.out.println("Usage: java S3MultipartUploadUtility <endpointUrl> <region> <accessKey> <secretKey> <bucketName> <filePath> <uploadSize>");
            return;
        }

        String endpointUrl = args[0];
        String region = args[1]; // Parse the region argument
        String accessKey = args[2];
        String secretKey = args[3];
        String bucketName = args[4];
        String filePath = args[5];
        long uploadSize = Long.parseLong(args[6]);

        BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKey, secretKey);
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AmazonS3ClientBuilder.EndpointConfiguration(endpointUrl, region)) // Use the region argument
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .withPathStyleAccessEnabled(true)
                .build();

        performMultipartUpload(s3Client, bucketName, filePath, uploadSize);
    }

    private static void performMultipartUpload(AmazonS3 s3Client, String bucketName, String filePath, long partSize) throws IOException {
        File file = new File(filePath);
        String key = file.getName();

        InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest(bucketName, key);
        InitiateMultipartUploadResult initiateResult = s3Client.initiateMultipartUpload(initiateRequest);
        String uploadId = initiateResult.getUploadId();

        List<PartETag> partETags = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] buffer = new byte[(int) partSize];
            int bytesRead;
            int partNumber = 1;

            while ((bytesRead = fis.read(buffer)) > 0) {
                UploadPartRequest uploadPartRequest = new UploadPartRequest()
                        .withBucketName(bucketName)
                        .withKey(key)
                        .withUploadId(uploadId)
                        .withPartNumber(partNumber)
                        .withInputStream(new FileInputStream(file))
                        .withPartSize(bytesRead);

                UploadPartResult uploadPartResult = s3Client.uploadPart(uploadPartRequest);
                partETags.add(uploadPartResult.getPartETag());

                partNumber++;
            }
        }

        CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(
                bucketName, key, uploadId, partETags);
        s3Client.completeMultipartUpload(completeRequest);
        System.out.println("Multipart upload completed successfully.");
    }
}
