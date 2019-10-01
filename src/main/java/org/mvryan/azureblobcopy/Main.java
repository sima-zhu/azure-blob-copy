package org.mvryan.azureblobcopy;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlockBlobClient;
import com.azure.storage.blob.ContainerClient;
import com.azure.storage.common.credentials.SharedKeyCredential;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(final String[] args) {
        LOG.info("App started");

        Options options = new Options();
        options.addOption("c", "config", true,"Path to configuration file");
        options.addOption("n", "account-name", true, "Azure account name");
        options.addOption("k", "account-key", true, "Azure account key");
        options.addOption("b", "container", true, "Azure container");
        options.addOption("s", "source", true, "Source blob to copy from");
        options.addOption("d", "destination", true, "Destination blob to copy to");
        options.addOption("h", "help", false,"Print a help message");

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine commandLine = parser.parse(options, args);
            if (! commandLine.hasOption("source") || ! commandLine.hasOption("destination")) {
                System.out.println("Both 'source' and 'destination' are required");
                printHelp(options);
            }
            else if (! commandLine.hasOption("config") &&
                    ! (commandLine.hasOption("account-name") && commandLine.hasOption("account-key") && commandLine.hasOption("container"))) {
                System.out.println("Either 'config' or 'account-name' + 'account-key' + 'container' are required");
                printHelp(options);
            }
            else {
                String source = commandLine.getOptionValue("source");
                String dest = commandLine.getOptionValue("destination");
                String accountName;
                String accountKey;
                String containerName;
                if (commandLine.hasOption("config")) {
                    Properties props = new Properties();
                    props.load(new FileInputStream(new File(commandLine.getOptionValue("config"))));
                    accountName = props.getProperty("account-name");
                    accountKey = props.getProperty("account-key");
                    containerName = props.getProperty("container");
                }
                else {
                    accountName = commandLine.getOptionValue("account-name");
                    accountKey = commandLine.getOptionValue("account-key");
                    containerName = commandLine.getOptionValue("container");
                }

                LOG.info("Copying {} to {} in account {} and container {}", source, dest, accountName, containerName);

                ContainerClient containerClient = getContainerClient(accountName, accountKey, containerName);
                doAzureCopy(source, dest, containerClient);
            }
        }
        catch (ParseException e) {
            LOG.error(e.getMessage(), e);
            printHelp(options);
        }
        catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private static ContainerClient getContainerClient(@NotNull final String accountName,
                                                      @NotNull final String accountKey,
                                                      @NotNull final String containerName) throws MalformedURLException {
        SharedKeyCredential credential = new SharedKeyCredential(accountName, accountKey);
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .endpoint(new URL(String.format("https://%s.blob.core.windows.net", accountName)).toString())
                .credential(credential)
                .buildClient();
        return blobServiceClient.getContainerClient(containerName);
    }

    private static void doAzureCopy(@NotNull final String sourceBlobName,
                                    @NotNull final String destBlobName,
                                    @NotNull final ContainerClient containerClient) {
        BlockBlobClient source = containerClient.getBlockBlobClient(sourceBlobName);
        if (! source.exists()) {
            LOG.error("Cannot copy from {} - blob does not exist");
            return;
        }
        long sourceSize = source.getProperties().blobSize();

        BlockBlobClient dest = containerClient.getBlockBlobClient(destBlobName);
        dest.copyFromURL(source.getBlobUrl());

        long destSize = dest.getProperties().blobSize();
        if (sourceSize != destSize) {
            LOG.error("Copy succeeded, but destination size != source size");
            return;
        }

        try {
            File srcTemp = File.createTempFile("asbc", "-src");
            File dstTemp = File.createTempFile("asbc", "-dst");
            srcTemp.deleteOnExit();
            dstTemp.deleteOnExit();

            downloadBlob(source, srcTemp);
            downloadBlob(dest, dstTemp);

            MessageDigest digest = MessageDigest.getInstance("SHA-256");

            String srcHash = getFileSha(srcTemp, digest);
            String dstHash = getFileSha(dstTemp, digest);

            if (! dstHash.equals(srcHash)) {
                LOG.error("Copy succeeded, but content is not the same");
                LOG.error("Source hash:       {}", srcHash);
                LOG.error("Destination hash:  {}", dstHash);
            }
        }
        catch (IOException | NoSuchAlgorithmException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private static void downloadBlob(@NotNull BlockBlobClient c, @NotNull File f) {
        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(f))) {
            try (InputStream in = c.openInputStream()) {
                IOUtils.copy(in, out);
            }
            catch (IOException e1) {
                LOG.error(e1.getMessage(), e1);
            }
        }
        catch (IOException e2) {
            LOG.error(e2.getMessage(), e2);
        }
    }

    private static String getFileSha(@NotNull final File f, @NotNull final MessageDigest digest) throws IOException {
        byte[] buf;
        try (InputStream in = new BufferedInputStream(new FileInputStream(f))) {
            buf = in.readNBytes((int) f.length());
            return new String(digest.digest(buf));
        }
    }

    private static void printHelp(Options options) {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("azure-storage-blob-copy", options);
        System.out.println("'source' and 'destination' are both required - and can be the same value");
        System.out.println("Either 'config' or 'account-name' + 'account-key' + 'container' is required");
        System.out.println("If 'config' is provided, 'account-name' and 'account-key' must be defined in the file in Java properties syntax");
    }
}
