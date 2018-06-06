/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"

#define  REPLICA_NUMBER            3
#define  QUORUM_NUMBER             2
#define  TRANSACTION_TIME_LIMIT   10
#define  STABLIZER_TRANS          -1

typedef tuple<Message, int, int> Transaction;

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */
class MP2Node {
private:
	// Vector holding the next two neighbors in the ring who have my replicas
	vector<Node> hasMyReplicas;
	// Vector holding the previous two neighbors in the ring whose replicas I have
	vector<Node> haveReplicasOf;
	// Ring
	vector<Node> ring;
	// Hash Table
	HashTable * ht;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet * emulNet;
	// Object of Log
	Log * log;
	vector<Node>::iterator myPosition;

	bool isCoordinator = false;

	map<int, Transaction> transactions;
	void msg_create_handler(Message *msg);
	void msg_read_handler(Message *msg);
	void msg_update_handler(Message *msg);
	void msg_delete_handler(Message *msg);
	void msg_reply_handler(Message *msg);
	void msg_readreply_handler(Message *msg);

	Message *get_transaction(int transId);
	MessageType get_transaction_type(int transId);

	int trans_success_counter(int transId);
	int trans_timeout_counter(int transId);
	void trans_invalidate(int transId);

	vector<Node> findNeighbors(vector<Node> ringOfNodes);
	void setNeighbors();
	bool isSameNode(Node n1, Node n2);
	void check_for_timeout();

public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	Member * getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);
	void findNeighbors();

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	// receive messages from Emulnet
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);

	// handle messages from receiving queue
	void checkMessages();

	// coordinator dispatches messages to corresponding nodes
	void dispatchMessages(Message message);

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
	bool createKeyValue(string key, string value, ReplicaType replica);
	string readKey(string key);
	bool updateKeyValue(string key, string value, ReplicaType replica);
	bool deletekey(string key);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol(vector<Node> curNeighbors);

	~MP2Node();
};

#endif /* MP2NODE_H_ */
