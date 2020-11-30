package org.apache.hadoop.hdfs.server.balancer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.shell.FsCommand;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.protocol.DataNodeUsageReport;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.hash.Hash;

import java.io.*;
import java.net.Socket;
import java.util.*;

import static org.apache.hadoop.hdfs.protocolPB.PBHelperClient.vintPrefixed;

public class FileBalancer {

    private final Configuration configuration;

    public FileBalancer(Configuration configuration) {
        this.configuration = configuration;
    }

    public boolean moveBlocks(List<NameNodeConnector> connectors) {
        Map<String, List<BlockFromFile>> locationMap = ReadFromFile();
        for (NameNodeConnector nameNodeConnector : connectors) {
            DistributedFileSystem distributedFileSystem = nameNodeConnector.getDistributedFileSystem();
            DFSClient dfsClient = distributedFileSystem.getClient();

            for (Map.Entry<String, List<BlockFromFile>> filesMap : locationMap.entrySet()) {
                String fileNameExt = filesMap.getKey();
                String fileName = fileNameExt.substring(0, fileNameExt.lastIndexOf("."));
                String path = "/user/hive/warehouse/" + fileName + "/" + fileNameExt;
                printIntoMoveBlocksLog("\n\nFile: " + path);

                List<BlockFromFile> blockList = filesMap.getValue();

                try {
                    FileSystem fileSystem = FileSystem.get(configuration);
                    if (fileSystem != null) {
                        ContentSummary contentSummary = fileSystem.getContentSummary(new Path(path));
                        long fileSize = contentSummary.getLength();
                        int i = 0;
                        List<LocatedBlock> locatedBlocks = dfsClient.getLocatedBlocks(path, 0, fileSize).getLocatedBlocks();
                        for (LocatedBlock block : locatedBlocks) {
                            List<String> nodes = blockList.get(i++).getNodes();
                            List<DatanodeStorageReport> targetNodes = new ArrayList<>();

                            try {
                                dfsClient.setReplication(path, (short) nodes.size());
                            } catch (IOException e) {
                                printIntoMoveBlocksLog("Set new Replication factor Error");
                                return true; //Return true if node not exists and then exit
                            }

                            for (String s : nodes) {
                                DatanodeStorageReport node = checkIfTargetNodeExists(nameNodeConnector, s);
                                if (node != null) {
                                    targetNodes.add(node);
                                } else {
                                    printIntoMoveBlocksLog("Node is NULL");
                                    return true; //Return true if node not exists and then exit
                                }
                            }

                            DatanodeInfo[] sourceNodes = getBlockSourceNode(block);
                            Map<DatanodeInfo, DatanodeStorageReport> replicaToMove = checkIfBlocksAreAlreadyInPosition(sourceNodes, targetNodes, nodes);

                            if (!replicaToMove.isEmpty()) {
                                for (Map.Entry<DatanodeInfo, DatanodeStorageReport> map : replicaToMove.entrySet()) {
                                    BlockToMove blockToMove = new BlockToMove(map.getKey(), map.getValue(), block, nameNodeConnector);
                                    if (!moveBlock(blockToMove)) {
                                        printIntoMoveBlocksLog("Block move error");
                                        return true; //Return true if moveBlock fail and then exit
                                    }
                                }
                            } else {
                                printIntoMoveBlocksLog("All Blocks are already in position");
                            }
                        }
                    }
                } catch (IOException e) {
                    printIntoMoveBlocksLog("File " + fileNameExt + "not found");
                }
            }
        }
        return false;
    }

    private Map<DatanodeInfo, DatanodeStorageReport> checkIfBlocksAreAlreadyInPosition(DatanodeInfo[] sourceNodes,
                                                                                       List<DatanodeStorageReport> targetNodes,
                                                                                       List<String> targetNodesString) {

        List<DatanodeInfo> sourcesList = new LinkedList<>(Arrays.asList(sourceNodes));
        for (DatanodeInfo sourceNode : sourceNodes) {
            if (targetNodesString.contains(sourceNode.getName())) {
                targetNodesString.remove(sourceNode.getName());
                sourcesList.remove(sourceNode);
            }
        }

        List<DatanodeStorageReport> targetsList = new ArrayList<>();
        for (DatanodeStorageReport datanodeStorageReport : targetNodes) {
            if (targetNodesString.contains(datanodeStorageReport.getDatanodeInfo().getName())) {
                targetsList.add(datanodeStorageReport);
            }
        }

        Map<DatanodeInfo, DatanodeStorageReport> map = new HashMap<>();
        for (int i = 0; i < sourcesList.size(); i++) {
            map.put(sourcesList.get(i), targetsList.get(i));
        }
        return map;
    }

    private DatanodeStorageReport checkIfTargetNodeExists(NameNodeConnector nameNodeConnector, String targetNode) {
        DatanodeStorageReport node = null;
        try {
            DatanodeStorageReport[] datanodeStorageReportList = nameNodeConnector.getLiveDatanodeStorageReport();
            for (DatanodeStorageReport datanodeStorageReport : datanodeStorageReportList) {
                if (datanodeStorageReport.getDatanodeInfo().getName().equals(targetNode)) {
                    node = datanodeStorageReport;
                }
            }
        } catch (IOException e) {
        }
        return node;
    }

    //TODO How Hadoop choose replica?
    private DatanodeInfo[] getBlockSourceNode(LocatedBlock locatedBlock) {
        return locatedBlock.getLocations();
    }

    //TODO Add Check on StorageType
    private boolean moveBlock(BlockToMove blockToMove) {

        printIntoMoveBlocksLog(blockToMove.toString());

        Socket sock = new Socket();
        DataOutputStream out = null;
        DataInputStream in = null;

        SaslDataTransferClient saslClient = new SaslDataTransferClient(configuration,
                DataTransferSaslUtil.getSaslPropertiesResolver(configuration),
                TrustedChannelResolver.getInstance(configuration), blockToMove.nameNodeConnector.fallbackToSimpleAuth);

        int ioFileBufferSize = DFSUtilClient.getIoFileBufferSize(configuration);

        DatanodeInfo datanodeInfo = blockToMove.dataNodeTarget.getDatanodeInfo();
        ExtendedBlock extendedBlock = blockToMove.block.getBlock();

        try {
            sock.connect(NetUtils.createSocketAddr(datanodeInfo.getName()), HdfsConstants.READ_TIMEOUT);

            sock.setSoTimeout(HdfsConstants.READ_TIMEOUT * 5);
            sock.setKeepAlive(true);

            OutputStream unbufOut = sock.getOutputStream();
            InputStream unbufIn = sock.getInputStream();

            final KeyManager km = blockToMove.nameNodeConnector.getKeyManager();
            Token<BlockTokenIdentifier> accessToken = km.getAccessToken(extendedBlock,
                    new StorageType[]{blockToMove.block.getStorageTypes()[0]}, new String[0]);

            IOStreamPair saslStreams = saslClient.socketSend(sock, unbufOut, unbufIn, km, accessToken, datanodeInfo);
            unbufOut = saslStreams.out;
            unbufIn = saslStreams.in;
            out = new DataOutputStream(new BufferedOutputStream(unbufOut, ioFileBufferSize));
            in = new DataInputStream(new BufferedInputStream(unbufIn, ioFileBufferSize));

            sendRequest(out, blockToMove, accessToken);
            receiveResponse(in);

            blockToMove.nameNodeConnector.getBytesMoved().addAndGet(extendedBlock.getNumBytes());

            printIntoMoveBlocksLog("- Block Moved");
        } catch (IOException e) {
            printIntoMoveBlocksLog("- Move Failed");
            return false;
        } finally {
            IOUtils.closeStream(out);
            IOUtils.closeStream(in);
            IOUtils.closeSocket(sock);
        }
        return true;
    }

    /** Send a reportedBlock replace request to the output stream */
    private void sendRequest(DataOutputStream out, BlockToMove blockToMove,
                             Token<BlockTokenIdentifier> accessToken) throws IOException {
        new Sender(out).replaceBlock(blockToMove.block.getBlock(), blockToMove.block.getStorageTypes()[0],
                accessToken, blockToMove.dataNodeSource.getDatanodeUuid(), blockToMove.dataNodeSource,
                null);
    }

    /** Receive a reportedBlock copy response from the input stream */
    private void receiveResponse(DataInputStream in) throws IOException {
        long startTime = Time.monotonicNow();
        DataTransferProtos.BlockOpResponseProto response =
                DataTransferProtos.BlockOpResponseProto.parseFrom(vintPrefixed(in));
        while (response.getStatus() == DataTransferProtos.Status.IN_PROGRESS) {
            // read intermediate responses
            response = DataTransferProtos.BlockOpResponseProto.parseFrom(vintPrefixed(in));
            // Stop waiting for slow block moves. Even if it stops waiting,
            // the actual move may continue.
            if (stopWaitingForResponse(startTime)) {
                throw new IOException("Block move timed out");
            }
        }
        String logInfo = "reportedBlock move is failed";
        DataTransferProtoUtil.checkBlockOpStatus(response, logInfo, true);
    }

    /** Check whether to continue waiting for response */
    private boolean stopWaitingForResponse(long startTime) {
        final int blockMoveTimeout = configuration.getInt(
                DFSConfigKeys.DFS_BALANCER_BLOCK_MOVE_TIMEOUT,
                DFSConfigKeys.DFS_BALANCER_BLOCK_MOVE_TIMEOUT_DEFAULT);

        return (blockMoveTimeout > 0 && (Time.monotonicNow() - startTime > blockMoveTimeout));
    }

    private Map<String, List<BlockFromFile>> ReadFromFile() {
        Map<String, List<BlockFromFile>> map = new HashMap<>();
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(System.getProperty("user.home")
                    + File.separator + "test-data" + File.separator + "FilesLocation.txt"));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] tmp = line.split(",");
                if (tmp.length < 2) break;
                String fileName = tmp[0];
                BlockFromFile blockFromFile = new BlockFromFile(new ArrayList<>(Arrays.asList(tmp).subList(1, tmp.length)));
                List<BlockFromFile> blockList;
                if (map.containsKey(fileName)) {
                    blockList = map.get(fileName);
                    blockList.add(blockFromFile);
                    map.replace(fileName, blockList);
                } else {
                    blockList = new ArrayList<>();
                    blockList.add(blockFromFile);
                    map.put(fileName, blockList);
                }
            }
            bufferedReader.close();
        } catch (IOException e) {
            printIntoMoveBlocksLog("File FileLocation.txt not found");
        }
        return map;
    }

    private void printIntoMoveBlocksLog(String line) {
        File file = new File(System.getProperty("user.home") + File.separator + "MoveBlocks.txt");
        FileWriter fr = null;
        try {
            fr = new FileWriter(file, true);
            fr.write(line + "\n");
            fr.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static class BlockFromFile {
        private List<String> nodes;

        public BlockFromFile(List<String> nodes) {
            this.nodes = nodes;
        }

        public List<String> getNodes() {
            return nodes;
        }

        public void setNodes(List<String> nodes) {
            this.nodes = nodes;
        }
    }

    private static class BlockToMove {
        private DatanodeInfo dataNodeSource;
        private DatanodeStorageReport dataNodeTarget;
        private LocatedBlock block;
        private NameNodeConnector nameNodeConnector;

        public BlockToMove(DatanodeInfo dataNodeSource, DatanodeStorageReport dataNodeTarget,
                           LocatedBlock block, NameNodeConnector nameNodeConnector) {
            this.dataNodeSource = dataNodeSource;
            this.dataNodeTarget = dataNodeTarget;
            this.block = block;
            this.nameNodeConnector = nameNodeConnector;
        }

        public String toString() {
            return "Moving block: " + block.getBlock().getBlockName() +
                    " From: " + dataNodeSource.getName() +
                    " -> To: " + dataNodeTarget.getDatanodeInfo().getName();
        }
    }

}
