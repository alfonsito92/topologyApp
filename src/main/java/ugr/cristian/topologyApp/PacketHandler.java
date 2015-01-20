/**
Copyright (C) 2015  Cristian Alfonso Prieto Sánchez

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package ugr.cristian.topologyApp;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

import org.opendaylight.controller.sal.action.Action;
import org.opendaylight.controller.sal.action.Output;
import org.opendaylight.controller.sal.action.SetDlDst;
import org.opendaylight.controller.sal.action.SetDlSrc;
import org.opendaylight.controller.sal.action.SetNwDst;
import org.opendaylight.controller.sal.action.SetNwSrc;
import org.opendaylight.controller.sal.core.ConstructionException;
import org.opendaylight.controller.sal.core.Node;
import org.opendaylight.controller.sal.core.NodeConnector;
import org.opendaylight.controller.sal.flowprogrammer.Flow;
import org.opendaylight.controller.sal.flowprogrammer.IFlowProgrammerService;
import org.opendaylight.controller.sal.match.Match;
import org.opendaylight.controller.sal.match.MatchType;
import org.opendaylight.controller.sal.packet.Ethernet;
import org.opendaylight.controller.sal.packet.IDataPacketService;
import org.opendaylight.controller.sal.packet.IListenDataPacket;
import org.opendaylight.controller.sal.packet.IPv4;
import org.opendaylight.controller.sal.packet.Packet;
import org.opendaylight.controller.sal.packet.PacketResult;
import org.opendaylight.controller.sal.packet.RawPacket;
import org.opendaylight.controller.sal.packet.TCP;
import org.opendaylight.controller.sal.utils.EtherTypes;
import org.opendaylight.controller.sal.utils.Status;
import org.opendaylight.controller.switchmanager.ISwitchManager;
import org.opendaylight.controller.topologymanager.ITopologyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PacketHandler implements IListenDataPacket {

    private static final Logger log = LoggerFactory.getLogger(PacketHandler.class);
    private static final String PUBLIC_IP = "10.0.0.100";
    private static final int SERVICE_PORT = 7777;
    private static final String SERVER1_IP = "10.0.0.1";
    private static final String SERVER2_IP = "10.0.0.2";
    private static final byte[] SERVER1_MAC = {0,0,0,0,0,0x01};
    private static final byte[] SERVER2_MAC = {0,0,0,0,0,0x02};
    private static final byte[] SERVICE_MAC = {0,0,0,0,0,0x64};
    private static final String SERVER1_CONNECTOR_NAME = "s1-eth1";
    private static final String SERVER2_CONNECTOR_NAME = "s1-eth2";

    private IDataPacketService dataPacketService;
    private IFlowProgrammerService flowProgrammerService;
    private ISwitchManager switchManager;
    private ITopologyManager topologyManager;
    private InetAddress publicInetAddress;
    private InetAddress server1Address;
    private InetAddress server2Address;
    private int serverNumber = 0;

    static private InetAddress intToInetAddress(int i) {
        byte b[] = new byte[] { (byte) ((i>>24)&0xff), (byte) ((i>>16)&0xff), (byte) ((i>>8)&0xff), (byte) (i&0xff) };
        InetAddress addr;
        try {
            addr = InetAddress.getByAddress(b);
        } catch (UnknownHostException e) {
            return null;
        }

        return addr;
    }

    public PacketHandler() {
        try {
            publicInetAddress = InetAddress.getByName(PUBLIC_IP);
        } catch (UnknownHostException e) {
            log.error(e.getMessage());
        }

        try {
            server1Address = InetAddress.getByName(SERVER1_IP);
        } catch (UnknownHostException e) {
            log.error(e.getMessage());
        }

        try {
            server2Address = InetAddress.getByName(SERVER2_IP);
        } catch (UnknownHostException e) {
            log.error(e.getMessage());
        }
    }

    /**
     * Sets a reference to the requested DataPacketService
     */
    void setDataPacketService(IDataPacketService s) {
        log.trace("Set DataPacketService.");

        dataPacketService = s;
    }

    /**
     * Unsets DataPacketService
     */
    void unsetDataPacketService(IDataPacketService s) {
        log.trace("Removed DataPacketService.");

        if (dataPacketService == s) {
            dataPacketService = null;
        }
    }

    /**
     * Sets a reference to the requested FlowProgrammerService
     */
    void setFlowProgrammerService(IFlowProgrammerService s) {
        log.trace("Set FlowProgrammerService.");

        flowProgrammerService = s;
    }

    /**
     * Unsets FlowProgrammerService
     */
    void unsetFlowProgrammerService(IFlowProgrammerService s) {
        log.trace("Removed FlowProgrammerService.");

        if (flowProgrammerService == s) {
            flowProgrammerService = null;
        }
    }

    /**
     * Sets a reference to the requested SwitchManagerService
     */
    void setSwitchManagerService(ISwitchManager s) {
        log.trace("Set SwitchManagerService.");

        switchManager = s;
    }

    /**
     * Unsets SwitchManagerService
     */
    void unsetSwitchManagerService(ISwitchManager s) {
        log.trace("Removed SwitchManagerService.");

        if (switchManager == s) {
            switchManager = null;
        }
    }

    /**
    * Sets a reference to the requested TopologyManagerService
    */
    void setTopologyManagerService(ITopologyManager s) {
      log.trace("Set TopologyManagerService.");

      topologyManager = s;
    }

    /**
    * Unsets SwitchManagerService
    */
    void unsetTopologyManagerService(ITopologyManager s) {
      log.trace("Removed TologyManagerService.");

      if (topologyManager == s) {
        topologyManager = null;
      }
    }


    @Override
    public PacketResult receiveDataPacket(RawPacket inPkt) {
        // The connector, the packet came from ("port")
        NodeConnector ingressConnector = inPkt.getIncomingNodeConnector();
        // The node that received the packet ("switch")
        Node node = ingressConnector.getNode();

        log.trace("Packet from " + node.getNodeIDString() + " " + ingressConnector.getNodeConnectorIDString());

        // Use DataPacketService to decode the packet.
        Packet pkt = dataPacketService.decodeDataPacket(inPkt);

        if (pkt instanceof Ethernet) {
            Ethernet ethFrame = (Ethernet) pkt;
            Object l3Pkt = ethFrame.getPayload();

            if (l3Pkt instanceof IPv4) {
                IPv4 ipv4Pkt = (IPv4) l3Pkt;
                InetAddress clientAddr = intToInetAddress(ipv4Pkt.getSourceAddress());
                InetAddress dstAddr = intToInetAddress(ipv4Pkt.getDestinationAddress());
                Object l4Datagram = ipv4Pkt.getPayload();

                if (l4Datagram instanceof TCP) {
                    TCP tcpDatagram = (TCP) l4Datagram;
                    int clientPort = tcpDatagram.getSourcePort();
                    int dstPort = tcpDatagram.getDestinationPort();

                    if (publicInetAddress.equals(dstAddr) && dstPort == SERVICE_PORT) {
                        log.info("Received packet for load balanced service");

                        // Select one of the two servers round robin.

                        InetAddress serverInstanceAddr;
                        byte[] serverInstanceMAC;
                        NodeConnector egressConnector;

                        // Synchronize in case there are two incoming requests at the same time.
                        synchronized (this) {
                            if (serverNumber == 0) {
                                log.info("Server 1 is serving the request");
                                serverInstanceAddr = server1Address;
                                serverInstanceMAC = SERVER1_MAC;
                                egressConnector = switchManager.getNodeConnector(node, SERVER1_CONNECTOR_NAME);
                                serverNumber = 1;
                            } else {
                                log.info("Server 2 is serving the request");
                                serverInstanceAddr = server2Address;
                                serverInstanceMAC = SERVER2_MAC;
                                egressConnector = switchManager.getNodeConnector(node, SERVER2_CONNECTOR_NAME);
                                serverNumber = 0;
                            }
                        }

                        // Create flow table entry for further incoming packets

                        // Match incoming packets of this TCP connection
                        // (4 tuple source IP, source port, destination IP, destination port)
                        Match match = new Match();
                        match.setField(MatchType.DL_TYPE, (short) 0x0800);  // IPv4 ethertype
                        match.setField(MatchType.NW_PROTO, (byte) 6);       // TCP protocol id
                        match.setField(MatchType.NW_SRC, clientAddr);
                        match.setField(MatchType.NW_DST, dstAddr);
                        match.setField(MatchType.TP_SRC, (short) clientPort);
                        match.setField(MatchType.TP_DST, (short) dstPort);

                        // List of actions applied to the packet
                        List<Action> actions = new LinkedList<Action>();

                        // Re-write destination IP to server instance IP
                        actions.add(new SetNwDst(serverInstanceAddr));

                        // Re-write destination MAC to server instance MAC
                        actions.add(new SetDlDst(serverInstanceMAC));

                        // Output packet on port to server instance
                        actions.add(new Output(egressConnector));

                        // Create the flow
                        Flow flow = new Flow(match, actions);

                        // Use FlowProgrammerService to program flow.
                        Status status = flowProgrammerService.addFlow(node, flow);
                        if (!status.isSuccess()) {
                            log.error("Could not program flow: " + status.getDescription());
                            return PacketResult.CONSUME;
                        }

                        // Create flow table entry for response packets from server to client

                        // Match outgoing packets of this TCP connection
                        match = new Match();
                        match.setField(MatchType.DL_TYPE, (short) 0x0800);
                        match.setField(MatchType.NW_PROTO, (byte) 6);
                        match.setField(MatchType.NW_SRC, serverInstanceAddr);
                        match.setField(MatchType.NW_DST, clientAddr);
                        match.setField(MatchType.TP_SRC, (short) dstPort);
                        match.setField(MatchType.TP_DST, (short) clientPort);

                        // Re-write the server instance IP address to the public IP address
                        actions = new LinkedList<Action>();
                        actions.add(new SetNwSrc(publicInetAddress));
                        actions.add(new SetDlSrc(SERVICE_MAC));

                        // Output to client port from which packet was received
                        actions.add(new Output(ingressConnector));

                        flow = new Flow(match, actions);
                        status = flowProgrammerService.addFlow(node, flow);
                        if (!status.isSuccess()) {
                            log.error("Could not program flow: " + status.getDescription());
                            return PacketResult.CONSUME;
                        }

                        // Forward initial packet to selected server

                        log.trace("Forwarding packet to " + serverInstanceAddr.toString() + " through port " + egressConnector.getNodeConnectorIDString());
                        ethFrame.setDestinationMACAddress(serverInstanceMAC);
                        ipv4Pkt.setDestinationAddress(serverInstanceAddr);
                        inPkt.setOutgoingNodeConnector(egressConnector);
                        dataPacketService.transmitDataPacket(inPkt);

                        return PacketResult.CONSUME;
                    }
                }
            }
        }

        // We did not process the packet -> let someone else do the job.
        return PacketResult.IGNORED;
    }

}
