import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class Main {
    public static void main(String[] args) {
        // Configure the S3 client
        S3AsyncClient s3AsyncClient = S3AsyncClient.builder()
                .region(Region.US_EAST_2)  // Change to your region
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build();

        String bucketName = "your-s3-bucket";  // Replace with your exact bucket name
        S3Uploader s3Uploader = new S3Uploader(s3AsyncClient, bucketName);

        // Define file details
        ObjectId fileId = new ObjectId("1234");
        Date uploadDate = new Date();
        String contentType = "text/plain";
        String objectKey = "anObjectKey";

        // Path to the file you want to upload
        Path filePath = Paths.get("path\\toyourfile");  // Replace with the path to your file

        try {
            // Define metadata
            Map<String, String> metaData = new HashMap<>();
            metaData.put("author", "John");

            // Upload the file using multipart upload
            CompletableFuture<FSFile> result = s3Uploader.uploadFile(fileId, uploadDate, contentType, objectKey, filePath, metaData);
            FSFile fsFile = result.get();

            // Print the result
            System.out.println("File uploaded successfully. ETag: " + fsFile.hash);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            s3AsyncClient.close();
        }
    }
}