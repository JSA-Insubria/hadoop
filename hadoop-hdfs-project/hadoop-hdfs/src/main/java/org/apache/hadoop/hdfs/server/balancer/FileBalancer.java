package org.apache.hadoop.hdfs.server.balancer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
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
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;

import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdfs.protocolPB.PBHelperClient.vintPrefixed;

public class FileBalancer {

    private Configuration configuration;

    public FileBalancer(Configuration configuration) {
        this.configuration = configuration;
    }

    public boolean moveBlocks(List<NameNodeConnector> connectors) {
        HashMap<String, String> locationMap = ReadFromFile();
            for (NameNodeConnector nameNodeConnector : connectors) {
                DistributedFileSystem distributedFileSystem = nameNodeConnector.getDistributedFileSystem();
                DFSClient dfsClient = distributedFileSystem.getClient();

                for (Map.Entry<String, String> map : locationMap.entrySet()) {
                    String fileNameExt = map.getKey();
                    String fileName = fileNameExt.substring(0, fileNameExt.lastIndexOf("."));
                    String nodeTarget = map.getValue();
                    String path = "/user/hive/warehouse/" + fileName + "/" + fileNameExt;
                    printIntoMoveBlocksLog("\n\nFile: " + path);

                    DatanodeStorageReport node = checkIfTargetNodeExists(nameNodeConnector, nodeTarget);
                    if (node != null) {
                        try {
                            for (LocatedBlock block : dfsClient.getLocatedBlocks(path, 0).getLocatedBlocks()) {
                                BlockToMove blockToMove = new BlockToMove(getBlockSourceNode(block), node, block, nameNodeConnector);
                                if (!checkIfBlockIsAlreadyInPosition(blockToMove)) {
                                    if (!moveBlock(blockToMove)) {
                                        return true; //Return true if moveBlock fail and then exit
                                    }
                                }
                                else {
                                    printIntoMoveBlocksLog("Block " + blockToMove.block.getBlock().getBlockName()
                                            + " already in node target");
                                }
                            }
                        } catch (IOException e) {
                            printIntoMoveBlocksLog("File " + fileNameExt + "not found");
                        }
                    }
                    else {
                        printIntoMoveBlocksLog("Target node not found");
                    }
                }
            }
        return false;
    }

    private boolean checkIfBlockIsAlreadyInPosition(BlockToMove blockToMove) {
        for (DatanodeInfo datanodeInfo : blockToMove.block.getLocations()) {
            if (blockToMove.dataNodeTarget.getDatanodeInfo().getName().equals(datanodeInfo.getName())) {
                return true;
            }
        }
        return false;
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
            printIntoMoveBlocksLog("Cluster offline");
        }
        return node;
    }

    //TODO How Hadoop choose replica?
    private DatanodeInfo getBlockSourceNode(LocatedBlock locatedBlock) {
        return locatedBlock.getLocations()[0];
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

    private HashMap<String, String> ReadFromFile() {
        HashMap<String, String> hashMap = new HashMap<>();
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(System.getProperty("user.home")
                    + File.separator + "FilesLocation.txt"));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] tmp = line.split(",");
                hashMap.put(tmp[0].trim(), tmp[1].trim());
            }
            bufferedReader.close();
        } catch (IOException e) {
            printIntoMoveBlocksLog("File FileLocation.txt not found");
        }
        return hashMap;
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
