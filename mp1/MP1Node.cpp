/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
//#include "Member.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() = default;

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TPING;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode, id, (short)port);

    return 0;
}

void MP1Node::initMemberListTable(Member *memberNode, int id, short port) {
    memberNode->memberList.clear();
    MemberListEntry me = MemberListEntry(id, port, memberNode->heartbeat, par->getcurrtime());
    memberNode->memberList.push_back(me);
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
    return 1;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size) {
	/*
	 * Your code goes here
	 */
     
    auto *msg = (MessageHdr *) data;
    char *msgContent = data + sizeof(MessageHdr);
    MsgTypes typeOfMsg = msg->msgType;
    auto *source = (Address *) msgContent;

    if (typeOfMsg == JOINREQ) {
        //updateMembershipList(id, port, heartbeat, timestamp)
        updateMembershipList(*(int *)(source->addr), 
                                *(short *)(source->addr + 4), 
                                    *(long *)(msgContent + sizeof(Address)+1),
                                        par->getcurrtime());

        size_t replySize = sizeof(MessageHdr)+sizeof(Address)+sizeof(long)+1;
        auto *replyMsg = (MessageHdr *)malloc(replySize);
        replyMsg->msgType = JOINREP;


        memcpy((char *)(replyMsg + 1), &(memberNode->addr), sizeof(Address));
        memcpy((char *)(replyMsg) + 1 + sizeof(Address) + 1, &(memberNode->heartbeat), sizeof(long));

        emulNet->ENsend(&memberNode->addr, source, (char *) replyMsg, replySize);
        free(replyMsg);
    }

    else if (typeOfMsg == JOINREP) {
        memberNode->inGroup = true;
        updateMembershipList(*(int *)(source->addr), 
                                *(short *)(source->addr + 4), 
                                    *(long *)(msgContent + sizeof(Address) + 1),
                                        par->getcurrtime());
    }

    else if (typeOfMsg == PING) {
        //somthing
        size_t msgContentSize = size - sizeof(MessageHdr);
        auto totalSize = (int)(msgContentSize);
        auto rowSize = (int)(sizeof(Address) + sizeof(long));

        vector<MemberListEntry> receivedList = DeserializeData(msgContent, totalSize/rowSize);

        for (std::vector<MemberListEntry>::iterator it = receivedList.begin();it != receivedList.end();it++) {
            updateMembershipList(it->getid(), it->getport(), it->getheartbeat(), it->gettimestamp());
        }
    }
    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */
    if (memberNode->pingCounter == 0) {
        memberNode->heartbeat++;
        memberNode->memberList[0].heartbeat++;
        pingOtherNodes();
        memberNode->pingCounter = TPING;
    }
    else {
        memberNode->pingCounter--;
    }
    CheckFailure();

}

void MP1Node::CheckFailure() {
    Address peer;
    for (vector<MemberListEntry>::iterator it = memberNode->memberList.begin() + 1;
          it != memberNode->memberList.end(); it++) {

        if (par->getcurrtime() - it->gettimestamp() > TREMOVE) {

#ifdef DEBUGLOG
        peer = getAddress(it->getid(), it->getport());
        log->logNodeRemove(&memberNode->addr, &peer);
#endif
        memberNode->memberList.erase(it);
        it --;
        continue;
        }

        // suspect the peer has failed
        if(par->getcurrtime() - it->gettimestamp() > TFAIL) {
                it->setheartbeat(-1);
        }
    }
}

/**
 * FUNCTION NAME: updateMembershipList(id, port, heartbeat, timestamp)
 * 
 * DESCRIPTION: Update the membership list based on the ping msg received
 *
 *
 */
void MP1Node::updateMembershipList(int id, short port, long heartbeat, long timestamp) {
    Address entry_address = getAddress(id, port);
    for (std::vector<MemberListEntry>::iterator it = memberNode->memberList.begin();it != memberNode->memberList.end();it++) {
        if (getAddress(it->getid(), it->getport()) == entry_address) {
            
            //check if the node is failed
            if (heartbeat == -1) {
                it->setheartbeat(-1);
                return;
            }

            //check if the node is already failed according to the local record
            if (it->getheartbeat() == -1) {
                return;
            }

            if (it->getheartbeat() < heartbeat) {
                it->setheartbeat(heartbeat);
                it->settimestamp(par->getcurrtime());
                return;
            }
            return;
        }
    }

    //not in local list
    if (heartbeat == -1) {
            return;
    }
    MemberListEntry newMember = MemberListEntry(id, port, heartbeat, timestamp);
    memberNode->memberList.push_back(newMember);
}


/**
 * FUNCTION NAME: pingOtherNodes()
 * 
 * DESCRIPTION: Let the node ping other nodes in the group
 *
 *
 */
bool MP1Node::pingOtherNodes() {
    size_t msgSize = sizeof(MessageHdr) + ((sizeof(Address) + sizeof(long))*memberNode->memberList.size());
    auto *msgData = (MessageHdr *)malloc(msgSize);
    msgData->msgType = PING;

    SerializeData((char *)(msgData+1));

    Address destination;

    for (std::vector<MemberListEntry>::iterator it = memberNode->memberList.begin() + 1; 
                                            it != memberNode->memberList.end(); it++) {
        //get destination's address from id and port
        destination = getAddress(it->getid(), it->getport());
        //send the ping msg
        emulNet->ENsend(&memberNode->addr, &destination, (char *)msgData, msgSize);
    }

    free(msgData);
    return true;
}

/**
 * FUNCTION NAME: SerializeData()
 *
 * DESCRIPTION: This fuction is used for serializing the membership list before ping it to the other nodes. 
 */

char* MP1Node::SerializeData(char* buffer) {
    int blockIndex = 0;
    size_t blockSize = sizeof(Address) + sizeof(long);
    auto *block = (char *)malloc((int)(blockSize));

    for (std::vector<MemberListEntry>::iterator it = memberNode->memberList.begin();it != memberNode->memberList.end();
                it++, blockIndex += blockSize) {
        Address addrTobeSent = getAddress(it->getid(), it->getport());
        
        long temp_heartbeat = it->getheartbeat();
        memcpy(block, &addrTobeSent, sizeof(Address));
        memcpy(block + sizeof(Address), &temp_heartbeat, sizeof(long));

        memcpy(buffer + blockIndex, block, blockSize);
    }
    free(block);
    return buffer;
}


 /**
 * FUNCTION NAME: DeserializeData()
 *
 * DESCRIPTION: This function is the opposite process of serialization
 */

vector<MemberListEntry> MP1Node::DeserializeData(char* table, int rows) {
    vector<MemberListEntry> returnList;
    int blockSize = sizeof(Address) + sizeof(long);
    MemberListEntry block;

    for (int i = 0;i < rows; i++, table += blockSize) {

        auto *address = (Address *) table;
        int ansId;
        short ansPort;
        long ansHeartbeat;

        memcpy(&ansId, address->addr, sizeof(int));
        memcpy(&ansPort, &(address->addr[4]), sizeof(short));
        memcpy(&ansHeartbeat, table + sizeof(Address), sizeof(long));

        block.setid(ansId);
        block.setport(ansPort);
        block.setheartbeat(ansHeartbeat);
        block.settimestamp(par->getcurrtime());

        MemberListEntry result = MemberListEntry(block);
        returnList.push_back(result);

    }
    return returnList;
}

Address MP1Node::getAddress(int id, short port) {
        Address result_address;
        memcpy(&result_address.addr, &id, sizeof(int));
        memcpy(&result_address.addr[4], &port, sizeof(int));
        return result_address;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
