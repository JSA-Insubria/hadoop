package org.apache.hadoop.hdfs.server.balancer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FilesInfo {

    private Configuration configuration;
    private ObjectMapper objectMapper;
    private List<NameNodeConnector> nameNodeConnectors;

    public FilesInfo(List<NameNodeConnector> nameNodeConnectors, Configuration configuration) {
        this.nameNodeConnectors=nameNodeConnectors;
        this.configuration = configuration;
    }

    public void generateFilesInfo() {
        objectMapper = new ObjectMapper();
        try {
            FileSystem fileSystem = FileSystem.get(configuration);

            RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = fileSystem.listFiles(
                    new Path("/user/hive/warehouse/"), true);
            while (fileStatusRemoteIterator.hasNext()) {
                LocatedFileStatus locatedFileStatus = fileStatusRemoteIterator.next();
                Path filePath = locatedFileStatus.getPath();

                ContentSummary contentSummary = fileSystem.getContentSummary(filePath);
                long fileSize = contentSummary.getLength();

                BlockLocation[] blockLocations = locatedFileStatus.getBlockLocations();
                List<String> blockIdList = getBlockId(filePath.toUri().getRawPath());

                List<BlockLocationsWithId> blockLocationsWithIdList = new ArrayList<>();

                int i = 0;
                for (String blockId : blockIdList) {
                    blockLocationsWithIdList.add(new BlockLocationsWithId(blockId, blockLocations[i++]));
                }

                String[] tmp = filePath.toString().split("/");
                String fileName = tmp[tmp.length-1].substring(0, tmp[tmp.length-1].lastIndexOf("."));

                saveJsonFile(new SingleFile(fileName, filePath.toString(), fileSize, blockLocationsWithIdList));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private List<String> getBlockId(String path) {
        List<String> blockFileNames = new ArrayList<>();
        for (NameNodeConnector nameNodeConnector : nameNodeConnectors) {
            DistributedFileSystem distributedFileSystem = nameNodeConnector.getDistributedFileSystem();
            DFSClient dfsClient = distributedFileSystem.getClient();
            try {
                for (LocatedBlock block : dfsClient.getLocatedBlocks(path, 0).getLocatedBlocks()) {
                    blockFileNames.add(block.getBlock().getLocalBlock().getBlockName());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return blockFileNames;
    }

    private void saveJsonFile(SingleFile singleFile) {
        File jsonDirectory = new File(System.getProperty("user.home") + File.separator + "FilesInfo");
        if (!jsonDirectory.exists()) {
            jsonDirectory.mkdir();
        }
        if (jsonDirectory.exists()) {
            try {
                objectMapper.writerWithDefaultPrettyPrinter().writeValue(new File(jsonDirectory + File.separator + singleFile.getFileName() + ".json"), singleFile);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static class SingleFile {
        private String fileName;
        private String filePath;
        private long fileSize;
        private List<BlockLocationsWithId> blockLocationsWithIdList;

        public SingleFile(String fileName, String filePath, long fileSize, List<BlockLocationsWithId> blockLocationsWithIdList) {
            this.fileName = fileName;
            this.filePath = filePath;
            this.fileSize = fileSize;
            this.blockLocationsWithIdList = blockLocationsWithIdList;
        }

        public String getFileName() {
            return fileName;
        }

        public void setFileName(String fileName) {
            this.fileName = fileName;
        }

        public String getFilePath() {
            return filePath;
        }

        public void setFilePath(String filePath) {
            this.filePath = filePath;
        }

        public long getFileSize() {
            return fileSize;
        }

        public void setFileSize(long fileSize) {
            this.fileSize = fileSize;
        }

        public List<BlockLocationsWithId> getBlockLocations() {
            return blockLocationsWithIdList;
        }

        public void setBlockLocations(List<BlockLocationsWithId> blockLocations) {
            this.blockLocationsWithIdList = blockLocations;
        }
    }

    private static class BlockLocationsWithId {
        private String blockId;
        private BlockLocation blockLocation;

        public BlockLocationsWithId(String blockId, BlockLocation blockLocation) {
            this.blockId = blockId;
            this.blockLocation = blockLocation;
        }

        public String getBlockId() {
            return blockId;
        }

        public void setBlockId(String blockId) {
            this.blockId = blockId;
        }

        public BlockLocation getBlockLocation() {
            return blockLocation;
        }

        public void setBlockLocation(BlockLocation blockLocation) {
            this.blockLocation = blockLocation;
        }
    }

}
