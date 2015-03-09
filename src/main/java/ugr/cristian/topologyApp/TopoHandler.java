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

import java.util.ArrayList;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;


import org.opendaylight.controller.sal.action.Action;
import org.opendaylight.controller.sal.action.Output;
import org.opendaylight.controller.sal.action.SetDlDst;
import org.opendaylight.controller.sal.action.SetDlSrc;
import org.opendaylight.controller.sal.action.SetNwDst;
import org.opendaylight.controller.sal.action.SetNwSrc;
import org.opendaylight.controller.sal.core.ConstructionException;
import org.opendaylight.controller.sal.core.Node;
import org.opendaylight.controller.sal.core.Edge;
import org.opendaylight.controller.sal.core.Host;
import org.opendaylight.controller.sal.core.Path;
import org.opendaylight.controller.sal.core.Property;
import org.opendaylight.controller.sal.core.NodeConnector;
import org.opendaylight.controller.sal.flowprogrammer.Flow;
import org.opendaylight.controller.sal.flowprogrammer.IFlowProgrammerService;
import org.opendaylight.controller.sal.match.Match;
import org.opendaylight.controller.sal.match.MatchType;
import org.opendaylight.controller.sal.packet.BitBufferHelper;
import org.opendaylight.controller.sal.packet.Ethernet;
import org.opendaylight.controller.sal.packet.IDataPacketService;
import org.opendaylight.controller.sal.packet.IListenDataPacket;
import org.opendaylight.controller.sal.packet.IPv4;
import org.opendaylight.controller.sal.packet.ICMP;
import org.opendaylight.controller.sal.packet.Packet;
import org.opendaylight.controller.sal.packet.PacketResult;
import org.opendaylight.controller.sal.packet.RawPacket;
import org.opendaylight.controller.sal.packet.TCP;
import org.opendaylight.controller.sal.reader.NodeConnectorStatistics;
import org.opendaylight.controller.sal.utils.EtherTypes;
import org.opendaylight.controller.sal.utils.IPProtocols;
import org.opendaylight.controller.sal.utils.Status;
import org.opendaylight.controller.switchmanager.ISwitchManager;
import org.opendaylight.controller.topologymanager.ITopologyManager;
import org.opendaylight.controller.statisticsmanager.IStatisticsManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopoHandler implements IListenDataPacket {

    private static final Logger log = LoggerFactory.getLogger(TopoHandler.class);

    private IDataPacketService dataPacketService;
    private ISwitchManager switchManager;
    private IFlowProgrammerService flowProgrammerService;
    private IStatisticsManager statisticsManager;
    private ITopologyManager topologyManager;
    private Map <InetAddress, NodeConnector> listIP = new HashMap <InetAddress, NodeConnector>();
    private ConcurrentMap< Map<InetAddress, Long>, NodeConnector> listIPMAC = new ConcurrentHashMap<Map<InetAddress, Long>, NodeConnector>();
    private Map<Node, Set<Edge>> nodeEdgeTopology = new HashMap<Node, Set<Edge>>();
    private Map<Edge, Set<Property>> edgeProperties = new HashMap<Edge, Set<Property>>();
    private List<Host> hosts = new ArrayList<Host>();

    private short idleTimeOut = 30;
    private short hardTimeOut = 60;

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
     * Sets a reference to the requested StatisticsService
     */
    void setStatisticsManagerService(IStatisticsManager s) {
        log.trace("Set StatisticsManagerService.");

        statisticsManager = s;
    }

    /**
     * Unset StatisticsManager
     */
    void unsetStatisticsManagerService(IStatisticsManager s) {
        log.trace("Unset StatisticsManagerService.");

        if (  statisticsManager == s) {
            statisticsManager = null;
        }
    }

    /**
     * Sets a reference to the requested TopologyManager
     */
    void setTopologyManagerService(ITopologyManager s) {
        log.trace("Set TopologyManagerService.");

        topologyManager = s;
    }

    /**
     * Unset TopologyManager
     */
    void unsetTopologyManagerService(ITopologyManager s) {
        log.trace("Unset TopologyManagerService.");

        if (  topologyManager == s) {
            topologyManager = null;
        }
    }


    @Override
    public PacketResult receiveDataPacket(RawPacket inPkt) {
        // The connector, the packet came from ("port")
        NodeConnector ingressConnector = inPkt.getIncomingNodeConnector();
        // The node that received the packet ("switch")
        Node node = ingressConnector.getNode();
        // Use DataPacketService to decode the packet.
        Packet pkt = dataPacketService.decodeDataPacket(inPkt);

        if (pkt instanceof Ethernet) {

            Ethernet ethFrame = (Ethernet) pkt;
            byte[] srcMAC_B = (ethFrame).getSourceMACAddress();
            long srcMAC = BitBufferHelper.toNumber(srcMAC_B);
            byte[] dstMAC_B = (ethFrame).getDestinationMACAddress();
            long dstMAC = BitBufferHelper.toNumber(dstMAC_B);
            Object l3Pkt = ethFrame.getPayload();

            if (l3Pkt instanceof IPv4) {
                IPv4 ipv4Pkt = (IPv4) l3Pkt;
                InetAddress srcAddr = intToInetAddress(ipv4Pkt.getSourceAddress());
                InetAddress dstAddr = intToInetAddress(ipv4Pkt.getDestinationAddress());
                Object l4Datagram = ipv4Pkt.getPayload();

                //learnSourceIP(srcAddr, ingressConnector);
                //Checking if the pair IP-MAC are in the list
                Map<InetAddress, Long> srcIPMAC = new HashMap<InetAddress, Long>();
                srcIPMAC.clear();
                srcIPMAC.put(srcAddr,srcMAC);

                if(knowHost(srcIPMAC)==null){
                  learnSourceIPMAC(srcIPMAC, ingressConnector);
                }

                if (l4Datagram instanceof ICMP) {

                  ICMP icmpDatagram = (ICMP) l4Datagram;
                  Map<InetAddress, Long> dstIPMAC = new HashMap<InetAddress, Long>();
                  dstIPMAC.clear();
                  dstIPMAC.put(dstAddr,dstMAC);
                  //Checking ir we haven't got the connector yet
                  NodeConnector egressConnector = knowHost(dstIPMAC);

                  if(egressConnector==null){
                    //In case don't have the connector, we flood the packet
                    floodPacket(inPkt);

                  } else{

                    /*************************Calculo ruta**************************/

                    nodeEdgeTopology.clear();
                    edgeProperties.clear();

                    nodeEdgeTopology = topologyManager.getNodeEdges();
                    edgeProperties = topologyManager.getEdges();

                    /***************************************************************/
                    if(programFlow( srcAddr, srcMAC_B, dstAddr, dstMAC_B, egressConnector, node) ){

                      log.debug("Flujo instalado correctamente en el nodo " + node + " por el puerto " + egressConnector);

                    }
                    else{
                      log.debug("Error instalando el flujo");
                    }

                    log.debug("NodeConnectors con Host conectados: " + topologyManager.getNodeConnectorWithHost());

                    inPkt.setOutgoingNodeConnector(egressConnector);
                    this.dataPacketService.transmitDataPacket(inPkt);

                  }

                  return PacketResult.CONSUME;

                }
              }
            }
            // We did not process the packet -> let someone else do the job.
            return PacketResult.IGNORED;
    }

    private void floodPacket(RawPacket inPkt) {
        NodeConnector incoming_connector = inPkt.getIncomingNodeConnector();
        Node incoming_node = incoming_connector.getNode();

        Set<NodeConnector> nodeConnectors =
                this.switchManager.getUpNodeConnectors(incoming_node);

                for (NodeConnector p : nodeConnectors) {
                    if (!p.equals(incoming_connector)) {
                      try {
                        RawPacket destPkt = new RawPacket(inPkt);
                        destPkt.setOutgoingNodeConnector(p);
                        this.dataPacketService.transmitDataPacket(destPkt);
                    } catch (ConstructionException e2) {
                        continue;
                    }
                }
            }
        }

    /**
    With this function we can program a flow in a selected Node with different match and only one action, transmit
    for a selected nodeconnector. We select a lot of parameters like src and dst IP and MAC, protocol (ICMP) and
    timeOuts for the flow (idle and hard).

    */

    private boolean programFlow(InetAddress srcAddr, byte[] srcMAC_B, InetAddress dstAddr,
    byte[] dstMAC_B, NodeConnector outConnector, Node node) {

        Match match = new Match();
        match.setField(MatchType.DL_TYPE, (short) 0x0800);  // IPv4 ethertype
        match.setField(MatchType.NW_PROTO, IPProtocols.ICMP.byteValue());
        match.setField(MatchType.NW_SRC, srcAddr);
        match.setField(MatchType.NW_DST, dstAddr);
        match.setField(MatchType.DL_SRC, srcMAC_B);
        match.setField(MatchType.DL_DST, dstMAC_B);

        List<Action> actions = new ArrayList<Action>();
        actions.add(new Output(outConnector));

        Flow f = new Flow(match, actions);

        // Create the flow
        Flow flow = new Flow(match, actions);

        flow.setIdleTimeout(idleTimeOut);
        flow.setHardTimeout(hardTimeOut);

        // Use FlowProgrammerService to program flow.
        Status status = flowProgrammerService.addFlowAsync(node, flow);
        if (!status.isSuccess()) {
            log.error("Could not program flow: " + status.getDescription());
            return false;
        }
        else{
        return true;
      }

    }

    /**
    Deprecated
    Put in a List the relationship between IP and nodeconnector.
    */

    private void learnSourceIP(InetAddress srcIP, NodeConnector ingressConnector) {

      this.listIP.put(srcIP, ingressConnector);

    }

    /**
    Put in a ConcurrentMap the relation between IP-MAC and Nodeconnector.
    */

    private void learnSourceIPMAC(Map<InetAddress, Long> src, NodeConnector ingressConnector) {

      this.listIPMAC.put(src, ingressConnector);

    }

    /**
    Deprecated
    return the nodeconnector from a selected IP
    */

    private NodeConnector getOutConnector(InetAddress srcAddress) {

        return this.listIP.get(srcAddress);
    }

    /**
    Return a Nodeconnector from a pair IP-MAC
    */

    private NodeConnector knowHost(Map<InetAddress, Long> host){

        return this.listIPMAC.get(host);

    }

    /**
    Show differents options about the current topology
    */

    private void showTopology(){

      Map<Edge, Set<Property>> edges = this.topologyManager.getEdges();
      log.debug("El mapa de Edges es: " + edges);

    }

    /**
    Method whic travel throug set<Object>
    */

    private void walkSet(Set<Object> object){

      for (Iterator<Object> it = object.iterator(); it.hasNext(); ) {
         Object temp = it.next();
         this.log.debug("El objeto correspondiente a la posicion: " + it + "es " + temp);

      }

    }


}
