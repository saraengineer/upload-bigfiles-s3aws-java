import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class S3Uploader {

    private S3AsyncClient s3AsyncClient;
    private String bucketName;
    private static final long MAX_SINGLE_UPLOAD_SIZE = 5L * 1024 * 1024 * 1024; // 5 GB

    public S3Uploader(S3AsyncClient s3AsyncClient, String bucketName) {
        this.s3AsyncClient = s3AsyncClient;
        this.bucketName = bucketName;
    }

    public CompletableFuture<FSFile> uploadFile(
            ObjectId fileId,
            Date uploadDate,
            String contentType,
            String objectKey,
            Path filePath,
            Map<String, String> metaData) {

        try {
            long fileSize = Files.size(filePath);
            if (fileSize <= MAX_SINGLE_UPLOAD_SIZE) {
                return singleUpload(fileId, uploadDate, contentType, objectKey, filePath, metaData);
            } else {
                return multipartUpload(fileId, uploadDate, contentType, objectKey, filePath, metaData);
            }
        } catch (IOException e) {
            CompletableFuture<FSFile> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(e);
            return failedFuture;
        }
    }

    private CompletableFuture<FSFile> singleUpload(
            ObjectId fileId,
            Date uploadDate,
            String contentType,
            String objectKey,
            Path filePath,
            Map<String, String> metaData) {

        FSFile fsFile = documentAsFSFile(fileId, filePath.getFileName().toString(), filePath.toFile().length(), uploadDate, contentType, null, metaData);
        Map<String, String> s3Metadata = new java.util.HashMap<>();
        if (metaData != null) {
            for (Map.Entry<String, String> entry : metaData.entrySet()) {
                if (entry.getValue() == null) continue;
                s3Metadata.put(entry.getKey(), escapeUnicode(entry.getValue().trim()));
            }
        }

        try {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .contentType(contentType)
                    .metadata(s3Metadata)
                    .build();

            CompletableFuture<PutObjectResponse> future = s3AsyncClient.putObject(putObjectRequest, AsyncRequestBody.fromFile(filePath));

            return future.thenApply(response -> {
                fsFile.setHash(response.eTag());
                return fsFile;
            });

        } catch (Exception e) {
            CompletableFuture<FSFile> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(e);
            return failedFuture;
        }
    }

    private CompletableFuture<FSFile> multipartUpload(
            ObjectId fileId,
            Date uploadDate,
            String contentType,
            String objectKey,
            Path filePath,
            Map<String, String> metaData) {

        FSFile fsFile = documentAsFSFile(fileId, filePath.getFileName().toString(), filePath.toFile().length(), uploadDate, contentType, null, metaData);
        Map<String, String> s3Metadata = new java.util.HashMap<>();
        if (metaData != null) {
            for (Map.Entry<String, String> entry : metaData.entrySet()) {
                if (entry.getValue() == null) continue;
                s3Metadata.put(entry.getKey(), escapeUnicode(entry.getValue().trim()));
            }
        }

        int partSize = 20 * 1024 * 1024; // Set part size to 20 MB
        List<CompletedPart> completedParts = new ArrayList<>();
        int partNumber = 1;

        try {
            // 1. Initialize multipart upload
            CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .contentType(contentType)
                    .metadata(s3Metadata)
                    .build();

            CreateMultipartUploadResponse createMultipartUploadResponse = s3AsyncClient.createMultipartUpload(createMultipartUploadRequest).get();
            String uploadId = createMultipartUploadResponse.uploadId();
            System.out.println("Multipart upload initiated. Upload ID: " + uploadId);

            // 2. Read file and upload each part
            try (RandomAccessFile file = new RandomAccessFile(filePath.toFile(), "r")) {
                long fileSize = file.length();
                long position = 0;
                ByteBuffer buffer = ByteBuffer.allocate(partSize);

                System.out.println("Total file size: " + fileSize + " bytes");
                int totalParts = (int) Math.ceil((double) fileSize / partSize);
                System.out.println("Expected number of parts: " + totalParts);

                while (position < fileSize) {
                    file.seek(position);
                    int bytesRead = file.getChannel().read(buffer);
                    buffer.flip();

                    System.out.println("Uploading part " + partNumber + " at position " + position + " with size " + bytesRead);

                    UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                            .bucket(bucketName)
                            .key(objectKey)
                            .uploadId(uploadId)
                            .partNumber(partNumber)
                            .contentLength((long) bytesRead)
                            .build();

                    UploadPartResponse uploadPartResponse = s3AsyncClient.uploadPart(uploadPartRequest, AsyncRequestBody.fromByteBuffer(buffer)).get();

                    completedParts.add(CompletedPart.builder()
                            .partNumber(partNumber)
                            .eTag(uploadPartResponse.eTag())
                            .build());

                    buffer.clear();
                    position += bytesRead;
                    partNumber++;
                }
            }

            // 3. Complete multipart upload
            CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
                    .parts(completedParts)
                    .build();

            CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .uploadId(uploadId)
                    .multipartUpload(completedMultipartUpload)
                    .build();

            CompleteMultipartUploadResponse completeMultipartUploadResponse = s3AsyncClient.completeMultipartUpload(completeMultipartUploadRequest).get();
            System.out.println("Multipart upload completed. ETag: " + completeMultipartUploadResponse.eTag());

            fsFile.setHash(completeMultipartUploadResponse.eTag());
            return CompletableFuture.completedFuture(fsFile);

        } catch (Exception e) {
            e.printStackTrace();
            CompletableFuture<FSFile> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(e);
            return failedFuture;
        }
    }

    private FSFile documentAsFSFile(ObjectId id, String filename, long length, Date uploadDate, String contentType, String hash, Map<String, String> metadata) {
        return new FSFile(this, id, filename, length, uploadDate, contentType, hash, metadata);
    }

    protected String escapeUnicode(String str) {
        return str.chars()
                .mapToObj(c -> c > 127 ? String.format("\\u%04x", c) : Character.toString((char) c))
                .collect(Collectors.joining());
    }
}