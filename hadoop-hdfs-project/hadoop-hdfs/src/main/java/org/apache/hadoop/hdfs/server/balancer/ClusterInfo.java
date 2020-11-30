package org.apache.hadoop.hdfs.server.balancer;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ClusterInfo {

    private List<NameNodeConnector> nameNodeConnectors;
    private ObjectMapper objectMapper;

    public ClusterInfo(List<NameNodeConnector> nameNodeConnectors) {
        this.nameNodeConnectors = nameNodeConnectors;
    }

    public void generateClusterInfo() {
        objectMapper = new ObjectMapper();
        for (NameNodeConnector nameNodeConnector : nameNodeConnectors) {
            try {
                DatanodeStorageReport[] datanodeStorageReports = nameNodeConnector.getLiveDatanodeStorageReport();
                for (DatanodeStorageReport datanodeStorageReport : datanodeStorageReports) {
                    saveJsonFile(datanodeStorageReport.getDatanodeInfo());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void saveJsonFile(DatanodeInfo datanodeInfo) {
        File fileDir = new File(System.getProperty("user.home")
                + File.separator + "results"
                + File.separator + "namenode");
        if (!fileDir.exists()) {
            fileDir.mkdir();
        }
        File jsonDirectory = new File(fileDir + File.separator + "ClusterInfo");
        if (!jsonDirectory.exists()) {
            jsonDirectory.mkdir();
        }
        if (jsonDirectory.exists()) {
            try {
                objectMapper.writerWithDefaultPrettyPrinter().writeValue(new File(jsonDirectory + File.separator + datanodeInfo.getName() + ".json"), datanodeInfo);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
