/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include <boost/scoped_ptr.hpp>
#include <boost/thread.hpp>
#include <set>
#include <vector>

#include "mongo/db/operation_context_noop.h"
#include "mongo/db/repl/network_interface_mock.h"
#include "mongo/db/repl/repl_coordinator_external_state_mock.h"
#include "mongo/db/repl/repl_coordinator_impl.h"
#include "mongo/db/repl/repl_settings.h"
#include "mongo/db/repl/replica_set_config.h"
#include "mongo/db/repl/topology_coordinator_impl.h"
#include "mongo/db/write_concern_options.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"

namespace mongo {

    // So that you can ASSERT_EQUALS two OpTimes
    std::ostream& operator<<( std::ostream &s, const OpTime &ot ) {
        s << ot.toString();
        return s;
    }
    // So that you can ASSERT_EQUALS two Date_ts
    std::ostream& operator<<( std::ostream &s, const Date_t &t ) {
        s << t.toString();
        return s;
    }

namespace repl {
namespace {
    
    const Seconds zeroSecs(0);

    TEST(ReplicationCoordinator, StartupShutdown) {
        ReplSettings settings;
        // Make sure we think we're a replSet
        settings.replSet = "mySet/node1:12345,node2:54321";
        ReplicationCoordinatorImpl coordinator(settings,
                                               new ReplicationCoordinatorExternalStateMock);
        ReplicaSetConfig config;
        config.initialize((BSON("_id" << "mySet" <<
                                "version" << 2 <<
                                "members" << BSON_ARRAY(BSON("host" << "node1:12345" <<
                                                             "_id" << 0 )))));
        coordinator.setCurrentReplicaSetConfig(config, 0);
        coordinator.startReplication(new TopologyCoordinatorImpl(zeroSecs), 
                                     new NetworkInterfaceMock);
        coordinator.shutdown();
    }

    TEST(ReplicationCoordinator, AwaitReplicationNumberBaseCases) {
        ReplSettings settings;
        ReplicationCoordinatorImpl coordinator(settings,
                                               new ReplicationCoordinatorExternalStateMock);
        OperationContextNoop txn;
        OpTime time(1, 1);

        WriteConcernOptions writeConcern;
        writeConcern.wTimeout = WriteConcernOptions::kNoWaiting;
        writeConcern.wNumNodes = 2;

        // Because we didn't set ReplSettings.replSet, it will think we're a standalone so
        // awaitReplication will always work.
        ReplicationCoordinator::StatusAndDuration statusAndDur = coordinator.awaitReplication(
                &txn, time, writeConcern);
        ASSERT_OK(statusAndDur.status);

        // Now make us a master in master/slave
        coordinator.getSettings().master = true;

        writeConcern.wNumNodes = 0;
        writeConcern.wMode = "majority";
        // w:majority always works on master/slave
        statusAndDur = coordinator.awaitReplication(&txn, time, writeConcern);
        ASSERT_OK(statusAndDur.status);

        // Now make us a replica set
        coordinator.getSettings().replSet = "mySet/node1:12345,node2:54321";

        // Waiting for 1 nodes always works
        writeConcern.wNumNodes = 1;
        writeConcern.wMode = "";
        statusAndDur = coordinator.awaitReplication(&txn, time, writeConcern);
        ASSERT_OK(statusAndDur.status);
    }

    TEST(ReplicationCoordinator, AwaitReplicationNumberOfNodesNonBlocking) {
        ReplSettings settings;
        // Make sure we think we're a replSet
        settings.replSet = "mySet/node1:12345,node2:54321";
        ReplicationCoordinatorImpl coordinator(settings,
                                               new ReplicationCoordinatorExternalStateMock);
        ReplicaSetConfig config;
        config.initialize((BSON("_id" << "mySet" <<
                                "version" << 2 <<
                                "members" << BSON_ARRAY(BSON("host" << "node1:12345" <<
                                                             "_id" << 0 )))));
        coordinator.setCurrentReplicaSetConfig(config, 0);
        OperationContextNoop txn;

        OID client1 = OID::gen();
        OID client2 = OID::gen();
        OID client3 = OID::gen();
        OpTime time1(1, 1);
        OpTime time2(1, 2);

        WriteConcernOptions writeConcern;
        writeConcern.wTimeout = WriteConcernOptions::kNoWaiting;
        writeConcern.wNumNodes = 2;

        // 2 nodes waiting for time1
        ReplicationCoordinator::StatusAndDuration statusAndDur = coordinator.awaitReplication(
                &txn, time1, writeConcern);
        ASSERT_EQUALS(ErrorCodes::ExceededTimeLimit, statusAndDur.status);
        ASSERT_OK(coordinator.setLastOptime(&txn, client1, time1));
        statusAndDur = coordinator.awaitReplication(&txn, time1, writeConcern);
        ASSERT_EQUALS(ErrorCodes::ExceededTimeLimit, statusAndDur.status);
        ASSERT_OK(coordinator.setLastOptime(&txn, client2, time1));
        statusAndDur = coordinator.awaitReplication(&txn, time1, writeConcern);
        ASSERT_OK(statusAndDur.status);

        // 2 nodes waiting for time2
        statusAndDur = coordinator.awaitReplication(&txn, time2, writeConcern);
        ASSERT_EQUALS(ErrorCodes::ExceededTimeLimit, statusAndDur.status);
        ASSERT_OK(coordinator.setLastOptime(&txn, client2, time2));
        statusAndDur = coordinator.awaitReplication(&txn, time2, writeConcern);
        ASSERT_EQUALS(ErrorCodes::ExceededTimeLimit, statusAndDur.status);
        ASSERT_OK(coordinator.setLastOptime(&txn, client3, time2));
        statusAndDur = coordinator.awaitReplication(&txn, time2, writeConcern);
        ASSERT_OK(statusAndDur.status);

        // 3 nodes waiting for time2
        writeConcern.wNumNodes = 3;
        statusAndDur = coordinator.awaitReplication(&txn, time2, writeConcern);
        ASSERT_EQUALS(ErrorCodes::ExceededTimeLimit, statusAndDur.status);
        ASSERT_OK(coordinator.setLastOptime(&txn, client1, time2));
        statusAndDur = coordinator.awaitReplication(&txn, time2, writeConcern);
        ASSERT_OK(statusAndDur.status);
    }

    /**
     * Used to wait for replication in a separate thread without blocking execution of the test.
     * To use, set the optime and write concern to be passed to awaitReplication and then call
     * start(), which will spawn a thread that calls awaitReplication.  No calls may be made
     * on the ReplicationAwaiter instance between calling start and getResult().  After returning
     * from getResult(), you can call reset() to allow the awaiter to be reused for another
     * awaitReplication call.
     */
    class ReplicationAwaiter {
    public:

        ReplicationAwaiter(ReplicationCoordinatorImpl* replCoord, OperationContext* txn) :
            _replCoord(replCoord), _finished(false),
            _result(ReplicationCoordinator::StatusAndDuration(
                    Status::OK(), ReplicationCoordinator::Milliseconds(0))) {}

        void setOpTime(const OpTime& ot) {
            _optime = ot;
        }

        void setWriteConcern(const WriteConcernOptions& wc) {
            _writeConcern = wc;
        }

        // may block
        ReplicationCoordinator::StatusAndDuration getResult() {
            _thread->join();
            ASSERT(_finished);
            return _result;
        }

        void start() {
            ASSERT(!_finished);
            _thread.reset(new boost::thread(stdx::bind(&ReplicationAwaiter::_awaitReplication,
                                                       this)));
        }

        void reset() {
            ASSERT(_finished);
            _finished = false;
            _result = ReplicationCoordinator::StatusAndDuration(
                    Status::OK(), ReplicationCoordinator::Milliseconds(0));
        }

    private:

        void _awaitReplication() {
            OperationContextNoop txn;
            _result = _replCoord->awaitReplication(&txn, _optime, _writeConcern);
            _finished = true;
        }

        ReplicationCoordinatorImpl* _replCoord;
        bool _finished;
        OpTime _optime;
        WriteConcernOptions _writeConcern;
        ReplicationCoordinator::StatusAndDuration _result;
        boost::scoped_ptr<boost::thread> _thread;
    };

    TEST(ReplicationCoordinator, AwaitReplicationNumberOfNodesBlocking) {
        ReplSettings settings;
        // Make sure we think we're a replSet
        settings.replSet = "mySet/node1:12345,node2:54321";
        ReplicationCoordinatorImpl coordinator(settings,
                                               new ReplicationCoordinatorExternalStateMock);
        ReplicaSetConfig config;
        config.initialize((BSON("_id" << "mySet" <<
                                "version" << 2 <<
                                "members" << BSON_ARRAY(BSON("host" << "node1:12345" <<
                                                             "_id" << 0 )))));
        coordinator.setCurrentReplicaSetConfig(config, 0);
        OperationContextNoop txn;
        ReplicationAwaiter awaiter(&coordinator, &txn);

        OID client1 = OID::gen();
        OID client2 = OID::gen();
        OID client3 = OID::gen();
        OpTime time1(1, 1);
        OpTime time2(1, 2);

        WriteConcernOptions writeConcern;
        writeConcern.wTimeout = WriteConcernOptions::kNoTimeout;
        writeConcern.wNumNodes = 2;

        // 2 nodes waiting for time1
        awaiter.setOpTime(time1);
        awaiter.setWriteConcern(writeConcern);
        awaiter.start();
        ASSERT_OK(coordinator.setLastOptime(&txn, client1, time1));
        ASSERT_OK(coordinator.setLastOptime(&txn, client2, time1));
        ReplicationCoordinator::StatusAndDuration statusAndDur = awaiter.getResult();
        ASSERT_OK(statusAndDur.status);
        awaiter.reset();

        // 2 nodes waiting for time2
        awaiter.setOpTime(time2);
        awaiter.start();
        ASSERT_OK(coordinator.setLastOptime(&txn, client2, time2));
        ASSERT_OK(coordinator.setLastOptime(&txn, client3, time2));
        statusAndDur = awaiter.getResult();
        ASSERT_OK(statusAndDur.status);
        awaiter.reset();

        // 3 nodes waiting for time2
        writeConcern.wNumNodes = 3;
        awaiter.setWriteConcern(writeConcern);
        awaiter.start();
        ASSERT_OK(coordinator.setLastOptime(&txn, client1, time2));
        statusAndDur = awaiter.getResult();
        ASSERT_OK(statusAndDur.status);
        awaiter.reset();
    }

    TEST(ReplicationCoordinator, AwaitReplicationTimeout) {
        ReplSettings settings;
        // Make sure we think we're a replSet
        settings.replSet = "mySet/node1:12345,node2:54321";
        ReplicationCoordinatorImpl coordinator(settings,
                                               new ReplicationCoordinatorExternalStateMock);
        ReplicaSetConfig config;
        config.initialize((BSON("_id" << "mySet" <<
                                "version" << 2 <<
                                "members" << BSON_ARRAY(BSON("host" << "node1:12345" <<
                                                             "_id" << 0 )))));
        coordinator.setCurrentReplicaSetConfig(config, 0);
        OperationContextNoop txn;
        ReplicationAwaiter awaiter(&coordinator, &txn);

        OID client1 = OID::gen();
        OID client2 = OID::gen();
        OpTime time1(1, 1);
        OpTime time2(1, 2);

        WriteConcernOptions writeConcern;
        writeConcern.wTimeout = 50;
        writeConcern.wNumNodes = 2;

        // 2 nodes waiting for time2
        awaiter.setOpTime(time2);
        awaiter.setWriteConcern(writeConcern);
        awaiter.start();
        ASSERT_OK(coordinator.setLastOptime(&txn, client1, time1));
        ASSERT_OK(coordinator.setLastOptime(&txn, client2, time1));
        ReplicationCoordinator::StatusAndDuration statusAndDur = awaiter.getResult();
        ASSERT_EQUALS(ErrorCodes::ExceededTimeLimit, statusAndDur.status);
        awaiter.reset();
    }

    TEST(ReplicationCoordinator, AwaitReplicationShutdown) {
        ReplSettings settings;
        // Make sure we think we're a replSet
        settings.replSet = "mySet/node1:12345,node2:54321";
        ReplicationCoordinatorImpl coordinator(settings,
                                               new ReplicationCoordinatorExternalStateMock);
        ReplicaSetConfig config;
        config.initialize((BSON("_id" << "mySet" <<
                                "version" << 2 <<
                                "members" << BSON_ARRAY(BSON("host" << "node1:12345" <<
                                                             "_id" << 0 )))));
        coordinator.setCurrentReplicaSetConfig(config, 0);
        coordinator.startReplication(new TopologyCoordinatorImpl(zeroSecs), 
                                     new NetworkInterfaceMock);
        OperationContextNoop txn;
        ReplicationAwaiter awaiter(&coordinator, &txn);

        OID client1 = OID::gen();
        OID client2 = OID::gen();
        OpTime time1(1, 1);
        OpTime time2(1, 2);

        WriteConcernOptions writeConcern;
        writeConcern.wTimeout = WriteConcernOptions::kNoTimeout;
        writeConcern.wNumNodes = 2;

        // 2 nodes waiting for time2
        awaiter.setOpTime(time2);
        awaiter.setWriteConcern(writeConcern);
        awaiter.start();
        ASSERT_OK(coordinator.setLastOptime(&txn, client1, time1));
        ASSERT_OK(coordinator.setLastOptime(&txn, client2, time1));
        coordinator.shutdown();
        ReplicationCoordinator::StatusAndDuration statusAndDur = awaiter.getResult();
        ASSERT_EQUALS(ErrorCodes::ShutdownInProgress, statusAndDur.status);
        awaiter.reset();
    }

    TEST(ReplicationCoordinator, GetReplicationMode) {
        // should default to modeNone
        ReplSettings settings;
        ReplicationCoordinatorImpl coordinator(settings,
                                               new ReplicationCoordinatorExternalStateMock);
        ASSERT_EQUALS(ReplicationCoordinator::modeNone, coordinator.getReplicationMode());

        // modeMasterSlave if master set
        ReplSettings settings2;
        settings2.master = true;
        ReplicationCoordinatorImpl coordinator2(settings2,
                                                new ReplicationCoordinatorExternalStateMock);
        ASSERT_EQUALS(ReplicationCoordinator::modeMasterSlave, coordinator2.getReplicationMode());

        // modeMasterSlave if the slave flag was set
        ReplSettings settings3;
        settings3.slave = SimpleSlave;
        ReplicationCoordinatorImpl coordinator3(settings3,
                                                new ReplicationCoordinatorExternalStateMock);
        ASSERT_EQUALS(ReplicationCoordinator::modeMasterSlave, coordinator3.getReplicationMode());

        // modeReplSet only once config isInitialized
        ReplSettings settings4;
        settings4.replSet = "mySet/node1:12345";
        ReplicationCoordinatorImpl coordinator4(settings4,
                                                new ReplicationCoordinatorExternalStateMock);
        ASSERT_EQUALS(ReplicationCoordinator::modeNone, coordinator4.getReplicationMode());
        ReplicaSetConfig config;
        config.initialize((BSON("_id" << "mySet" <<
                                "version" << 2 <<
                                "members" << BSON_ARRAY(BSON("host" << "node1:12345" <<
                                                             "_id" << 0 )))));
        coordinator4.setCurrentReplicaSetConfig(config, 0);
        ASSERT_EQUALS(ReplicationCoordinator::modeReplSet, coordinator4.getReplicationMode());

        // modeReplSet only once config isInitialized even if the slave flag was set
        settings4.slave = SimpleSlave;
        ReplicationCoordinatorImpl coordinator5(settings4,
                                                new ReplicationCoordinatorExternalStateMock);
        ASSERT_EQUALS(ReplicationCoordinator::modeMasterSlave, coordinator5.getReplicationMode());
        coordinator5.setCurrentReplicaSetConfig(config, 0);
        ASSERT_EQUALS(ReplicationCoordinator::modeReplSet, coordinator5.getReplicationMode());
    }

    TEST(ReplicationCoordinator, AwaitReplicationNamedModes) {
        // TODO(spencer): Test awaitReplication with w:majority and tag groups
    }

    // This test fixture ensures that any tests that call startReplication on their coordinator
    // will always call shutdown on the coordinator as well.
    class ReplicationCoordinatorTestWithShutdown : public ::mongo::unittest::Test {
    public:
        virtual ~ReplicationCoordinatorTestWithShutdown() {
            if (coordinator.get()) {
                coordinator->shutdown();
            }
        }
        boost::scoped_ptr<ReplicationCoordinatorImpl> coordinator;
    };

    TEST_F(ReplicationCoordinatorTestWithShutdown, TestPrepareReplSetUpdatePositionCommand) {

        ReplSettings settings;
        settings.replSet = "mySet:/test1:1234,test2:1234,test3:1234";
        BSONObj configObj =
                BSON("_id" << "mySet" <<
                     "version" << 1 <<
                     "members" << BSON_ARRAY(BSON("_id" << 0 << "host" << "test1:1234") <<
                                             BSON("_id" << 1 << "host" << "test2:1234") <<
                                             BSON("_id" << 2 << "host" << "test3:1234")));
        ReplicaSetConfig rsConfig;
        ASSERT_OK(rsConfig.initialize(configObj));
        coordinator.reset(new ReplicationCoordinatorImpl(
                settings, new ReplicationCoordinatorExternalStateMock));
        coordinator->startReplication(new TopologyCoordinatorImpl(zeroSecs), 
                                      new NetworkInterfaceMock);
        coordinator->setCurrentReplicaSetConfig(rsConfig, 1);

        OID rid1 = OID::gen();
        OID rid2 = OID::gen();
        OID rid3 = OID::gen();
        HandshakeArgs handshake1;
        handshake1.initialize(BSON("handshake" << rid1 <<
                                   "member" << 0 <<
                                   "config" << BSON("_id" << 0 << "host" << "test1:1234")));
        HandshakeArgs handshake2;
        handshake2.initialize(BSON("handshake" << rid2 <<
                                   "member" << 1 <<
                                   "config" << BSON("_id" << 1 << "host" << "test2:1234")));
        HandshakeArgs handshake3;
        handshake3.initialize(BSON("handshake" << rid3 <<
                                   "member" << 2 <<
                                   "config" << BSON("_id" << 2 << "host" << "test3:1234")));
        OperationContextNoop txn;
        ASSERT_OK(coordinator->processHandshake(&txn, handshake1));
        ASSERT_OK(coordinator->processHandshake(&txn, handshake2));
        ASSERT_OK(coordinator->processHandshake(&txn, handshake3));
        OpTime optime1(1, 1);
        OpTime optime2(1, 2);
        OpTime optime3(2, 1);
        ASSERT_OK(coordinator->setLastOptime(&txn, rid1, optime1));
        ASSERT_OK(coordinator->setLastOptime(&txn, rid2, optime2));
        ASSERT_OK(coordinator->setLastOptime(&txn, rid3, optime3));

        // Check that the proper BSON is generated for the replSetUpdatePositionCommand
        BSONObjBuilder cmdBuilder;
        coordinator->prepareReplSetUpdatePositionCommand(&txn, &cmdBuilder);
        BSONObj cmd = cmdBuilder.done();

        ASSERT_EQUALS(2, cmd.nFields());
        ASSERT_EQUALS("replSetUpdatePosition", cmd.firstElement().fieldNameStringData());

        std::set<OID> rids;
        BSONForEach(entryElement, cmd["optimes"].Obj()) {
            BSONObj entry = entryElement.Obj();
            OID rid = entry["_id"].OID();
            rids.insert(rid);
            if (rid == rid1) {
                ASSERT_EQUALS(optime1, entry["optime"]._opTime());
            } else if (rid == rid2) {
                ASSERT_EQUALS(optime2, entry["optime"]._opTime());
            } else {
                ASSERT_EQUALS(rid3, rid);
                ASSERT_EQUALS(optime3, entry["optime"]._opTime());
            }
        }
        ASSERT_EQUALS(3U, rids.size()); // Make sure we saw all 3 nodes
    }

    TEST_F(ReplicationCoordinatorTestWithShutdown, TestHandshakes) {
        ReplSettings settings;
        settings.replSet = "mySet:/test1:1234,test2:1234,test3:1234";
        BSONObj configObj =
                BSON("_id" << "mySet" <<
                     "version" << 1 <<
                     "members" << BSON_ARRAY(BSON("_id" << 0 << "host" << "test1:1234") <<
                                             BSON("_id" << 1 << "host" << "test2:1234") <<
                                             BSON("_id" << 2 << "host" << "test3:1234")));
        ReplicaSetConfig rsConfig;
        ASSERT_OK(rsConfig.initialize(configObj));
        coordinator.reset(new ReplicationCoordinatorImpl(
                settings, new ReplicationCoordinatorExternalStateMock));
        coordinator->startReplication(new TopologyCoordinatorImpl(zeroSecs), 
                                      new NetworkInterfaceMock);
        coordinator->setCurrentReplicaSetConfig(rsConfig, 1);

        // Test generating basic handshake with no chaining
        OperationContextNoop txn;
        std::vector<BSONObj> handshakes;
        coordinator->prepareReplSetUpdatePositionCommandHandshakes(&txn, &handshakes);
        ASSERT_EQUALS(1U, handshakes.size());
        BSONObj handshakeCmd = handshakes[0];
        ASSERT_EQUALS(2, handshakeCmd.nFields());
        ASSERT_EQUALS("replSetUpdatePosition", handshakeCmd.firstElement().fieldNameStringData());
        BSONObj handshake = handshakeCmd["handshake"].Obj();
        ASSERT_EQUALS(coordinator->getMyRID(&txn), handshake["handshake"].OID());
        ASSERT_EQUALS(1, handshake["member"].Int());
        handshakes.clear();

        // Have other nodes handshake us and make sure we process it right.
        OID slave1RID = OID::gen();
        OID slave2RID = OID::gen();
        HandshakeArgs slave1Handshake;
        slave1Handshake.initialize(BSON("handshake" << slave1RID <<
                                        "member" << 0 <<
                                        "config" << BSON("_id" << 0 << "host" << "test1:1234")));
        HandshakeArgs slave2Handshake;
        slave2Handshake.initialize(BSON("handshake" << slave2RID <<
                                        "member" << 2 <<
                                        "config" << BSON("_id" << 2 << "host" << "test2:1234")));
        ASSERT_OK(coordinator->processHandshake(&txn, slave1Handshake));
        ASSERT_OK(coordinator->processHandshake(&txn, slave2Handshake));

        coordinator->prepareReplSetUpdatePositionCommandHandshakes(&txn, &handshakes);
        ASSERT_EQUALS(3U, handshakes.size());
        std::set<OID> rids;
        for (std::vector<BSONObj>::iterator it = handshakes.begin(); it != handshakes.end(); ++it) {
            BSONObj handshakeCmd = *it;
            ASSERT_EQUALS(2, handshakeCmd.nFields());
            ASSERT_EQUALS("replSetUpdatePosition",
                          handshakeCmd.firstElement().fieldNameStringData());

            BSONObj handshake = handshakeCmd["handshake"].Obj();
            OID rid = handshake["handshake"].OID();
            rids.insert(rid);
            if (rid == coordinator->getMyRID(&txn)) {
                ASSERT_EQUALS(1, handshake["member"].Int());
            } else if (rid == slave1RID) {
                ASSERT_EQUALS(0, handshake["member"].Int());
            } else {
                ASSERT_EQUALS(slave2RID, rid);
                ASSERT_EQUALS(2, handshake["member"].Int());
            }
        }
        ASSERT_EQUALS(3U, rids.size()); // Make sure we saw all 3 nodes
    }

    TEST_F(ReplicationCoordinatorTestWithShutdown, TestReplSetGetStatus) {
        // This test starts by configuring a ReplicationCoordinator as a member of a 4 node replica
        // set, with each node in a different state.
        // The first node is DOWN, as if we tried heartbeating them and it failed in some way.
        // The second node is in state SECONDARY, as if we've received a valid heartbeat from them.
        // The third node is in state UNKNOWN, as if we've not yet had any heartbeating activity
        // with them yet.  The fourth node is PRIMARY and corresponds to ourself, which gets its
        // information for replSetGetStatus from a different source than the nodes that aren't
        // ourself.  After this setup, we call replSetGetStatus and make sure that the fields
        // returned for each member match our expectations.
        ReplSettings settings;
        settings.replSet = "mySet:/test1:1234,test2:1234,test3:1234";
        BSONObj configObj =
                BSON("_id" << "mySet" <<
                     "version" << 1 <<
                     "members" << BSON_ARRAY(BSON("_id" << 0 << "host" << "test0:1234") <<
                                             BSON("_id" << 1 << "host" << "test1:1234") <<
                                             BSON("_id" << 2 << "host" << "test2:1234") <<
                                             BSON("_id" << 3 << "host" << "test3:1234")));
        ReplicaSetConfig rsConfig;
        ASSERT_OK(rsConfig.initialize(configObj));
        coordinator.reset(new ReplicationCoordinatorImpl(
                settings, new ReplicationCoordinatorExternalStateMock));
        TopologyCoordinatorImpl* topCoord = new TopologyCoordinatorImpl(zeroSecs);
        coordinator->startReplication(topCoord, new NetworkInterfaceMock);
        Date_t startupTime(curTimeMillis64());
        OpTime electionTime(1, 2);
        OpTime oplogProgress(3, 4);
        ReplicationExecutor::CallbackData cbd(NULL,
                                              ReplicationExecutor::CallbackHandle(),
                                              Status::OK());
        topCoord->updateConfig(cbd, rsConfig, 3, startupTime, oplogProgress);

        // Now that the replica set is setup, put the members into the states we want them in.
        MemberHeartbeatData member1hb(0);
        member1hb.setDownValues(startupTime, "");
        topCoord->updateHeartbeatData(startupTime, member1hb, 0, oplogProgress);
        MemberHeartbeatData member2hb(1);
        member2hb.setUpValues(
                startupTime, MemberState::RS_SECONDARY, electionTime, oplogProgress, "", "READY");
        topCoord->updateHeartbeatData(startupTime, member2hb, 1, oplogProgress);
        sleepsecs(1); // so uptime will be non-zero

        topCoord->_changeMemberState(MemberState::RS_PRIMARY);
        OperationContextNoop txn;
        coordinator->setLastOptime(&txn, coordinator->getMyRID(&txn), oplogProgress);

        // Now node 0 is down, node 1 is up, and for node 2 we have no heartbeat data yet.
        BSONObjBuilder statusBuilder;
        ASSERT_OK(coordinator->processReplSetGetStatus(&statusBuilder));
        BSONObj rsStatus = statusBuilder.obj();

        // Test results for all non-self members
        ASSERT_EQUALS("mySet", rsStatus["set"].String());
        ASSERT_LESS_THAN(startupTime.asInt64(), rsStatus["date"].Date().asInt64());
        std::vector<BSONElement> memberArray = rsStatus["members"].Array();
        ASSERT_EQUALS(4U, memberArray.size());
        BSONObj member0Status = memberArray[0].Obj();
        BSONObj member1Status = memberArray[1].Obj();
        BSONObj member2Status = memberArray[2].Obj();

        // Test member 0, the node that's DOWN
        ASSERT_EQUALS(0, member0Status["_id"].Int());
        ASSERT_EQUALS("test0:1234", member0Status["name"].String());
        ASSERT_EQUALS(0, member0Status["health"].Double());
        ASSERT_EQUALS(MemberState::RS_DOWN, member0Status["state"].Int());
        ASSERT_EQUALS("(not reachable/healthy)", member0Status["stateStr"].String());
        ASSERT_EQUALS(0, member0Status["uptime"].Int());
        ASSERT_EQUALS(OpTime(), OpTime(member0Status["optime"].timestampValue()));
        ASSERT_EQUALS(OpTime().asDate(), member0Status["optimeDate"].Date().millis);
        ASSERT_EQUALS(startupTime, member0Status["lastHeartbeat"].Date());
        ASSERT_EQUALS(Date_t(), member0Status["lastHeartbeatRecv"].Date());

        // Test member 1, the node that's SECONDARY
        ASSERT_EQUALS(1, member1Status["_id"].Int());
        ASSERT_EQUALS("test1:1234", member1Status["name"].String());
        ASSERT_EQUALS(1, member1Status["health"].Double());
        ASSERT_EQUALS(MemberState::RS_SECONDARY, member1Status["state"].Int());
        ASSERT_EQUALS(MemberState(MemberState::RS_SECONDARY).toString(),
                      member1Status["stateStr"].String());
        ASSERT_LESS_THAN_OR_EQUALS(1, member1Status["uptime"].Int());
        ASSERT_EQUALS(oplogProgress, OpTime(member1Status["optime"].timestampValue()));
        ASSERT_EQUALS(oplogProgress.asDate(), member1Status["optimeDate"].Date().millis);
        ASSERT_EQUALS(startupTime, member1Status["lastHeartbeat"].Date());
        ASSERT_EQUALS(Date_t(), member1Status["lastHeartbeatRecv"].Date());
        ASSERT_EQUALS("READY", member1Status["lastHeartbeatMessage"].String());

        // Test member 2, the node that's UNKNOWN
        ASSERT_EQUALS(2, member2Status["_id"].Int());
        ASSERT_EQUALS("test2:1234", member2Status["name"].String());
        ASSERT_EQUALS(-1, member2Status["health"].Double());
        ASSERT_EQUALS(MemberState::RS_UNKNOWN, member2Status["state"].Int());
        ASSERT_EQUALS(MemberState(MemberState::RS_UNKNOWN).toString(),
                      member2Status["stateStr"].String());
        ASSERT_FALSE(member2Status.hasField("uptime"));
        ASSERT_FALSE(member2Status.hasField("optime"));
        ASSERT_FALSE(member2Status.hasField("optimeDate"));
        ASSERT_FALSE(member2Status.hasField("lastHearbeat"));
        ASSERT_FALSE(member2Status.hasField("lastHearbeatRecv"));

        // Now test results for ourself, the PRIMARY
        ASSERT_EQUALS(MemberState::RS_PRIMARY, rsStatus["myState"].Int());
        BSONObj selfStatus = memberArray[3].Obj();
        ASSERT_TRUE(selfStatus["self"].Bool());
        ASSERT_EQUALS(3, selfStatus["_id"].Int());
        ASSERT_EQUALS("test3:1234", selfStatus["name"].String());
        ASSERT_EQUALS(1, selfStatus["health"].Double());
        ASSERT_EQUALS(MemberState::RS_PRIMARY, selfStatus["state"].Int());
        ASSERT_EQUALS(MemberState(MemberState::RS_PRIMARY).toString(),
                      selfStatus["stateStr"].String());
        ASSERT_EQUALS(1, selfStatus["uptime"].Int());
        ASSERT_EQUALS(oplogProgress, OpTime(selfStatus["optime"].timestampValue()));
        ASSERT_EQUALS(oplogProgress.asDate(), selfStatus["optimeDate"].Date().millis);

        // TODO(spencer): Test electionTime and pingMs are set properly
    }

    TEST_F(ReplicationCoordinatorTestWithShutdown, TestGetElectionId) {
        ReplSettings settings;
        settings.replSet = "mySet:/test1:1234,test2:1234,test3:1234";
        coordinator.reset(new ReplicationCoordinatorImpl(
                settings, new ReplicationCoordinatorExternalStateMock));
        TopologyCoordinatorImpl* topCoord = new TopologyCoordinatorImpl(zeroSecs);
        coordinator->startReplication(topCoord, new NetworkInterfaceMock);

        OID electionID1 = coordinator->getElectionId();
        topCoord->_changeMemberState(MemberState::RS_PRIMARY);
        OID electionID2 = coordinator->getElectionId();
        ASSERT_NOT_EQUALS(electionID1, electionID2);
    }

    // TODO(spencer): Unit test replSetFreeze

}  // namespace
}  // namespace repl
}  // namespace mongo
