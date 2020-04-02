package org.apache.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JobSubmitterInfo {

    private Configuration configuration;
    private JobID jobId;
    private Job job;

    public JobSubmitterInfo(Configuration configuration, JobID jobId, Job job) {
        this.configuration = configuration;
        this.jobId = jobId;
        this.job = job;
    }

    public void printJobInfo() {
        try {
            JobConf jConf = (JobConf)job.getConfiguration();
            org.apache.hadoop.mapred.InputSplit[] splits =
                    jConf.getInputFormat().getSplits(jConf, jConf.getNumMapTasks());

            List<String> paths = new ArrayList<>();
            List<String> locations = new ArrayList<>();
            for (Path path : FileInputFormat.getInputPaths(jConf)) {
                paths.add(path.toString());
            }
            for (org.apache.hadoop.mapred.InputSplit inputSplit : splits) {
                Collections.addAll(locations, inputSplit.getLocations());
            }

            List<JobDataPath> jobDataPathList = new ArrayList<>();
            FileSystem fileSystem = FileSystem.get(configuration);
            for (Path path : FileInputFormat.getInputPaths(jConf)) {
                jobDataPathList.add(new JobDataPath(path.toString(), getFileInfo(path, fileSystem)));
            }

            saveJsonFile(new JobPrint(jobId.toString(), paths, locations, jobDataPathList));

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private List<JobDataFile> getFileInfo(Path path, FileSystem fileSystem) {
        List<JobDataFile> jobDataFilesPathList = new ArrayList<>();
        try {
            RemoteIterator<LocatedFileStatus> remoteIterator = fileSystem.listLocatedStatus(path);
            while (remoteIterator.hasNext()) {
                LocatedFileStatus locatedFileStatus = remoteIterator.next();
                if(locatedFileStatus.isFile()) {
                    jobDataFilesPathList.add(new JobDataFile(locatedFileStatus.toString(),
                            getBlockInfo(locatedFileStatus.getBlockLocations())));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jobDataFilesPathList;
    }


    private List<JobDataBlock> getBlockInfo(BlockLocation[] blockLocations) {
        List<JobDataBlock> jobDataBlockList = new ArrayList<>();
        try {
            int blockIndex = 0;
            for (BlockLocation blockLocation : blockLocations) {
                List<JobDataReplica> jobDataReplicaList = new ArrayList<>();
                for (int i = 0; i < blockLocation.getNames().length; i++) {
                    jobDataReplicaList.add(new JobDataReplica((i+1),
                            blockLocation.getNames()[i],
                            blockLocation.getStorageIds()[i],
                            blockLocation.getStorageTypes()[i].name(),
                            blockLocation.getTopologyPaths()[i]));
                }
                jobDataBlockList.add(new JobDataBlock(blockIndex++, jobDataReplicaList));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return jobDataBlockList;
    }

    private void saveJsonFile(JobPrint jobPrint) {
        File jsonDirectory = new File(System.getProperty("user.home") + File.separator + "JobBlocks");
        if (!jsonDirectory.exists()) {
            jsonDirectory.mkdir();
        }
        if (jsonDirectory.exists()) {
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.writerWithDefaultPrettyPrinter().writeValue(new File(jsonDirectory +
                        File.separator + jobPrint.getJobName() + ".json"), jobPrint);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static class JobPrint {

        private String jobName;
        private List<String> paths;
        private List<String> locations;
        private List<JobDataPath> jobDataPaths;

        public JobPrint(String jobName, List<String> paths, List<String> locations, List<JobDataPath> jobDataPaths) {
            this.jobName = jobName;
            this.paths = paths;
            this.locations = locations;
            this.jobDataPaths = jobDataPaths;
        }

        public String getJobName() {
            return jobName;
        }

        public void setJobName(String jobName) {
            this.jobName = jobName;
        }

        public List<String> getPaths() {
            return paths;
        }

        public void setPaths(List<String> paths) {
            this.paths = paths;
        }

        public List<String> getLocations() {
            return locations;
        }

        public void setLocations(List<String> locations) {
            this.locations = locations;
        }

        public List<JobDataPath> getJobDataPaths() {
            return jobDataPaths;
        }

        public void setJobDataPaths(List<JobDataPath> jobDataPaths) {
            this.jobDataPaths = jobDataPaths;
        }
    }

    private static class JobDataPath {
        private String path;
        private List<JobDataFile> filesList;

        public JobDataPath(String path, List<JobDataFile> filesList) {
            this.path = path;
            this.filesList = filesList;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public List<JobDataFile> getFilesList() {
            return filesList;
        }

        public void setFilesList(List<JobDataFile> filesList) {
            this.filesList = filesList;
        }
    }

    private static class JobDataFile {
        private String file;
        private List<JobDataBlock> blockList;

        public JobDataFile(String file, List<JobDataBlock> blockList) {
            this.file = file;
            this.blockList = blockList;
        }

        public String getFile() {
            return file;
        }

        public void setFile(String file) {
            this.file = file;
        }

        public List<JobDataBlock> getBlockList() {
            return blockList;
        }

        public void setBlockList(List<JobDataBlock> blockList) {
            this.blockList = blockList;
        }
    }

    private static class JobDataBlock {
        private int blockId;
        private List<JobDataReplica> replicaList;

        public JobDataBlock(int blockId, List<JobDataReplica> replicaList) {
            this.blockId = blockId;
            this.replicaList = replicaList;
        }

        public int getBlockId() {
            return blockId;
        }

        public void setBlockId(int blockId) {
            this.blockId = blockId;
        }

        public List<JobDataReplica> getReplicaList() {
            return replicaList;
        }

        public void setReplicaList(List<JobDataReplica> replicaList) {
            this.replicaList = replicaList;
        }
    }

    private static class JobDataReplica {
        private int replicaId;
        private String location;
        private String storageId;
        private String storageType;
        private String topologyPath;

        public JobDataReplica(int replicaId, String location, String storageId, String storageType, String topologyPath) {
            this.replicaId = replicaId;
            this.location = location;
            this.storageId = storageId;
            this.storageType = storageType;
            this.topologyPath = topologyPath;
        }

        public int getReplicaId() {
            return replicaId;
        }

        public void setReplicaId(int replicaId) {
            this.replicaId = replicaId;
        }

        public String getLocation() {
            return location;
        }

        public void setLocation(String location) {
            this.location = location;
        }

        public String getStorageId() {
            return storageId;
        }

        public void setStorageId(String storageId) {
            this.storageId = storageId;
        }

        public String getStorageType() {
            return storageType;
        }

        public void setStorageType(String storageType) {
            this.storageType = storageType;
        }

        public String getTopologyPath() {
            return topologyPath;
        }

        public void setTopologyPath(String topologyPath) {
            this.topologyPath = topologyPath;
        }
    }

}
