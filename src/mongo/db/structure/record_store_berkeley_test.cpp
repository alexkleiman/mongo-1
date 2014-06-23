// record_store_berkeley_test.cpp

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

#include "mongo/db/structure/record_store_berkeley.h"

#include "mongo/db/operation_context_berkeley.h"
#include "mongo/db/storage/record.h"
#include "mongo/db/storage/berkeley/berkeley_recovery_unit.h"
#include "mongo/unittest/unittest.h"

using namespace mongo;

namespace {

    // allows us to declare StatusWith's without assigning them a real value
    DiskLoc dummyDiskLoc;
    StatusWith<DiskLoc> dummyStatusWith(dummyDiskLoc);
    std::string db_name = "berkeleytest";

    TEST(BerkeleyRecordStore, FullSimpleInsert1) {
        {
            OperationContextBerkeley txn;
            BerkeleyRecordStore rs(txn.getEnv(), db_name.data(), false, -1, -1, NULL);
            
            {
                WriteUnitOfWork wu(txn.recoveryUnit());
                StatusWith<DiskLoc> result = rs.insertRecord(&txn, "abc", 4, 1000);
                ASSERT_TRUE(result.isOK());
                RecordData record = rs.dataFor(result.getValue());
                ASSERT_EQUALS(string("abc"), string(record.data()));
            }
        }

        // Maybe make this cleanup cleaner in the future
        Db(NULL, 0).remove(("berkeleyEnv/" + db_name + ".db").data(), NULL, 0);
        DbEnv(0).remove("berkeleyEnv/", DB_FORCE);
        remove("berkeleyEnv/log.0000000001");
    }



    TEST(BerkeleyRecordStore, FullSimpleDelete1) {
        {
            OperationContextBerkeley txn;
            BerkeleyRecordStore rs(txn.getEnv(), db_name.data(), false, -1, -1, NULL);
            ASSERT_EQUALS(rs.numRecords(), 0);
            {
                WriteUnitOfWork wu(txn.recoveryUnit());

                StatusWith<DiskLoc> result = rs.insertRecord(&txn, "abc", 4, 1000);
                ASSERT_TRUE(result.isOK());
                RecordData record = rs.dataFor(result.getValue());
                ASSERT_EQUALS(string("abc"), string(record.data()));
                ASSERT_EQUALS(rs.numRecords(), 1);

                rs.deleteRecord(&txn, result.getValue());
                ASSERT_FALSE(rs.hasRecordFor(result.getValue()));
                ASSERT_EQUALS(rs.numRecords(), 0);
                ASSERT_EQUALS(rs.dataSize(), 0);
            }
        }

        // Maybe make this cleanup cleaner in the future
        Db(NULL, 0).remove(("berkeleyEnv/" + db_name + ".db").data(), NULL, 0);
        DbEnv(0).remove("berkeleyEnv/", DB_FORCE);
        remove("berkeleyEnv/log.0000000001");
    }

    TEST(BerkeleyRecordStore, UpdateSmaller1) {
        {
            OperationContextBerkeley txn;
            BerkeleyRecordStore rs(txn.getEnv(), db_name.data(), false, -1, -1, NULL);

            {
                WriteUnitOfWork wu(txn.recoveryUnit());
                StatusWith<DiskLoc> result = rs.insertRecord(&txn, "abc", 4, 1000);
                ASSERT_TRUE(result.isOK());
                RecordData record = rs.dataFor(result.getValue());
                ASSERT_EQUALS(string("abc"), string(record.data()));

                StatusWith<DiskLoc> result2 = rs.updateRecord(&txn, result.getValue(), "a", 2, 1000, NULL);
                ASSERT_TRUE(result2.isOK());
                RecordData record2 = rs.dataFor(result2.getValue());
                ASSERT_EQUALS(string("a"), string(record2.data()));
            }
        }

        // Maybe make this cleanup cleaner in the future
        Db(NULL, 0).remove(("berkeleyEnv/" + db_name + ".db").data(), NULL, 0);
        DbEnv(0).remove("berkeleyEnv/", DB_FORCE);
        remove("berkeleyEnv/log.0000000001");
    }

    TEST(BerkeleyRecordStore, UpdateLarger1) {
        {
            OperationContextBerkeley txn;
            BerkeleyRecordStore rs(txn.getEnv(), db_name.data(), false, -1, -1, NULL);
    
            {
                WriteUnitOfWork wu(txn.recoveryUnit());
    
                StatusWith<DiskLoc> result = rs.insertRecord(&txn, "abc", 4, 1000);
                ASSERT_TRUE(result.isOK());
                RecordData record = rs.dataFor(result.getValue());
                ASSERT_EQUALS(string("abc"), string(record.data()));
    
                StatusWith<DiskLoc> result2 = rs.updateRecord(&txn, result.getValue(),
                        "abcdef", 7, 1000, NULL);
                ASSERT_TRUE(result2.isOK());
                RecordData record2 = rs.dataFor(result2.getValue());
                ASSERT_EQUALS(string("abcdef"), string(record2.data()));
            }
        }

        // Maybe make this cleanup cleaner in the future
        Db(NULL, 0).remove(("berkeleyEnv/" + db_name + ".db").data(), NULL, 0);
        DbEnv(0).remove("berkeleyEnv/", DB_FORCE);
        remove("berkeleyEnv/log.0000000001");
    }

    TEST(BerkeleyRecordStore, MultipleUpdateLarge) {
        {
            OperationContextBerkeley txn;
            BerkeleyRecordStore rs(txn.getEnv(), db_name.data(), false, -1, -1, NULL);

            {
                WriteUnitOfWork wu(txn.recoveryUnit());

                const int large_data_length = 1000 * 1000;
                char large_data[large_data_length];
                for (int i = 0; i < large_data_length; i++)
                    large_data[i] = i % 255;


                const int original_data_length = 1000;
                char original_data[original_data_length];
                for (int i = 0; i < original_data_length; i++)
                    original_data[i] = i % 255;


                StatusWith<DiskLoc> result = rs.insertRecord(&txn, original_data,
                        original_data_length + 1, 1000);
                ASSERT_TRUE(result.isOK());
                RecordData record = rs.dataFor(result.getValue());
                ASSERT_EQUALS(original_data, string(record.data()));

                StatusWith<DiskLoc> result2 = rs.updateRecord(&txn, result.getValue(), "a", 2, 
                    1000, NULL);
                ASSERT_TRUE(result2.isOK());
                RecordData record2 = rs.dataFor(result2.getValue());
                ASSERT_EQUALS(string("a"), string(record2.data()));

                StatusWith<DiskLoc> result3 = rs.updateRecord(&txn, result.getValue(), large_data,
                        large_data_length + 1, 1000, NULL);
                ASSERT_TRUE(result3.isOK());
                RecordData record3 = rs.dataFor(result3.getValue());
                ASSERT_EQUALS(large_data, string(record3.data()));
            }
        }

        // Maybe make this cleanup cleaner in the future
        Db(NULL, 0).remove(("berkeleyEnv/" + db_name + ".db").data(), NULL, 0);
        DbEnv(0).remove("berkeleyEnv/", DB_FORCE);
        remove("berkeleyEnv/log.0000000001");
    }

    TEST(BerkeleyRecordStore, RollbackSingleInsertion) {
        {
            OperationContextBerkeley txn;
            BerkeleyRecordStore rs(txn.getEnv(), db_name.data(), false, -1, -1, NULL);

            StatusWith<DiskLoc> result = dummyStatusWith;

            {
                WriteUnitOfWork wu(txn.recoveryUnit());

                result = rs.insertRecord(&txn, "abc", 4, 1000);
                RecordData record = rs.dataFor(result.getValue());
                ASSERT_EQUALS(string("abc"), string(record.data()));
                ASSERT_EQUALS(rs.numRecords(), 1);
            } // calls wu's destructor, equivalent to saying wu._ru->endUnitOfWork();

            ASSERT_FALSE(rs.hasRecordFor(result.getValue()));
            ASSERT_EQUALS(rs.numRecords(), 0);
            ASSERT_EQUALS(rs.dataSize(), 0);
        }

        // Maybe make this cleanup cleaner in the future
        Db(NULL, 0).remove(("berkeleyEnv/" + db_name + ".db").data(), NULL, 0);
        DbEnv(0).remove("berkeleyEnv/", DB_FORCE);
        remove("berkeleyEnv/log.0000000001");
    }

    TEST(BerkeleyRecordStore, CommitSingleInsertion) {
        {
            OperationContextBerkeley txn;
            BerkeleyRecordStore rs(txn.getEnv(), db_name.data(), false, -1, -1, NULL);

            StatusWith<DiskLoc> result = dummyStatusWith;

            {
                WriteUnitOfWork wu(txn.recoveryUnit());

                result = rs.insertRecord(&txn, "abc", 4, 1000);
                RecordData record = rs.dataFor(result.getValue());
                ASSERT_EQUALS(string("abc"), string(record.data()));
                ASSERT_EQUALS(rs.numRecords(), 1);

                wu.commit();
            } // calls wu's destructor, equivalent to saying wu._ru->endUnitOfWork();

            RecordData newRecord = rs.dataFor(result.getValue());
            ASSERT_EQUALS(string("abc"), string(newRecord.data()));
            ASSERT_EQUALS(rs.numRecords(), 1);
        }

        // Maybe make this cleanup cleaner in the future
        Db(NULL, 0).remove(("berkeleyEnv/" + db_name + ".db").data(), NULL, 0);
        DbEnv(0).remove("berkeleyEnv/", DB_FORCE);
        remove("berkeleyEnv/log.0000000001");
    }

    TEST(BerkeleyRecordStore, RollbackSingleDeletion) {
        {
            OperationContextBerkeley txn;
            BerkeleyRecordStore rs(txn.getEnv(), db_name.data(), false, -1, -1, NULL);

            StatusWith<DiskLoc> result = dummyStatusWith;

            {
                WriteUnitOfWork wu(txn.recoveryUnit());

                result = rs.insertRecord(&txn, "abc", 4, 1000);
                ASSERT_TRUE(result.isOK());

                wu.commit();

                rs.deleteRecord(&txn, result.getValue());
                ASSERT_FALSE(rs.hasRecordFor(result.getValue()));
                ASSERT_EQUALS(rs.dataSize(), 0);
                ASSERT_EQUALS(rs.numRecords(), 0);

            } // calls wu's destructor, equivalent to saying wu._ru->endUnitOfWork();

            ASSERT_EQUALS(rs.numRecords(), 1);
            RecordData record = rs.dataFor(result.getValue());
            ASSERT_TRUE(rs.hasRecordFor(result.getValue()));
            ASSERT_EQUALS(string("abc"), string(record.data()));
        }

        // Maybe make this cleanup cleaner in the future
        Db(NULL, 0).remove(("berkeleyEnv/" + db_name + ".db").data(), NULL, 0);
        DbEnv(0).remove("berkeleyEnv/", DB_FORCE);
        remove("berkeleyEnv/log.0000000001");
    }

    TEST(BerkeleyRecordStore, CommitSingleDeletion) {
        {
            OperationContextBerkeley txn;
            BerkeleyRecordStore rs(txn.getEnv(), db_name.data(), false, -1, -1, NULL);

            StatusWith<DiskLoc> result = dummyStatusWith;

            {
                WriteUnitOfWork wu(txn.recoveryUnit());
                result = rs.insertRecord(&txn, "abc", 4, 1000);
                ASSERT_TRUE(result.isOK());

                wu.commit();

                rs.deleteRecord(&txn, result.getValue());
                wu.commit();
            } // calls wu's destructor, equivalent to saying wu._ru->endUnitOfWork();

            ASSERT_FALSE(rs.hasRecordFor(result.getValue()));
            ASSERT_EQUALS(rs.dataSize(), 0);
            ASSERT_EQUALS(rs.numRecords(), 0);
        }

        // Maybe make this cleanup cleaner in the future
        Db(NULL, 0).remove(("berkeleyEnv/" + db_name + ".db").data(), NULL, 0);
        DbEnv(0).remove("berkeleyEnv/", DB_FORCE);
        remove("berkeleyEnv/log.0000000001");
    }

    TEST(BerkeleyRecordStore, RollbackSingleUpdate) {
        {
            OperationContextBerkeley txn;
            BerkeleyRecordStore rs(txn.getEnv(), db_name.data(), false, -1, -1, NULL);

            StatusWith<DiskLoc> insertResult = dummyStatusWith;
            StatusWith<DiskLoc> updateResult = dummyStatusWith;

            {
                WriteUnitOfWork wu(txn.recoveryUnit());
                insertResult = rs.insertRecord(&txn, "abc", 4, 1000);
                ASSERT_TRUE(insertResult.isOK());

                wu.commit();

                updateResult = rs.updateRecord(&txn, insertResult.getValue(), "def", 4, 1000, NULL);

                ASSERT_TRUE(updateResult.isOK());
                RecordData record = rs.dataFor(updateResult.getValue());
                ASSERT_EQUALS(string("def"), string(record.data()));

            } // calls wu's destructor, equivalent to saying wu._ru->endUnitOfWork();

            RecordData newRecord = rs.dataFor(insertResult.getValue());
            ASSERT_EQUALS(string("abc"), string(newRecord.data()));
            ASSERT_EQUALS(rs.numRecords(), 1);
            ASSERT_TRUE(insertResult.getValue() == updateResult.getValue()
                    || !rs.hasRecordFor(updateResult.getValue()));
        }

        // Maybe make this cleanup cleaner in the future
        Db(NULL, 0).remove(("berkeleyEnv/" + db_name + ".db").data(), NULL, 0);
        DbEnv(0).remove("berkeleyEnv/", DB_FORCE);
        remove("berkeleyEnv/log.0000000001");
    }

    TEST(BerkeleyRecordStore, CommitSingleUpdate) {
        {
            OperationContextBerkeley txn;
            BerkeleyRecordStore rs(txn.getEnv(), db_name.data(), false, -1, -1, NULL);

            StatusWith<DiskLoc> insertResult = dummyStatusWith;
            StatusWith<DiskLoc> updateResult = dummyStatusWith;

            {
                WriteUnitOfWork wu(txn.recoveryUnit());
                insertResult = rs.insertRecord(&txn, "abc", 4, 1000);
                ASSERT_TRUE(insertResult.isOK());

                wu.commit();

                updateResult = rs.updateRecord(&txn, insertResult.getValue(), "def", 4, 1000, NULL);

                ASSERT_TRUE(updateResult.isOK());
                RecordData record = rs.dataFor(updateResult.getValue());
                ASSERT_EQUALS(string("def"), string(record.data()));

                wu.commit();
            } // calls wu's destructor, equivalent to saying wu._ru->endUnitOfWork();

            // make sure that the update persisted
            RecordData updatedRecord = rs.dataFor(updateResult.getValue());
            ASSERT_EQUALS(string("def"), string(updatedRecord.data()));
            ASSERT_TRUE(insertResult.getValue() == updateResult.getValue()
                    || !rs.hasRecordFor(insertResult.getValue()));
        }

        // Maybe make this cleanup cleaner in the future
        Db(NULL, 0).remove(("berkeleyEnv/" + db_name + ".db").data(), NULL, 0);
        DbEnv(0).remove("berkeleyEnv/", DB_FORCE);
        remove("berkeleyEnv/log.0000000001");
    }

    TEST(BerkeleyRecordStore, MultiCommitsMixed) {
        {
            OperationContextBerkeley txn;
            BerkeleyRecordStore rs(txn.getEnv(), db_name.data(), false, -1, -1, NULL);

            StatusWith<DiskLoc> firstInsertResult = dummyStatusWith;
            StatusWith<DiskLoc> secondInsertResult = dummyStatusWith;
            StatusWith<DiskLoc> thirdInsertResult = dummyStatusWith;
            StatusWith<DiskLoc> fourthInsertResult = dummyStatusWith;
            StatusWith<DiskLoc> firstUpdateResult = dummyStatusWith;
            StatusWith<DiskLoc> secondUpdateResult = dummyStatusWith;

            {
                WriteUnitOfWork wu(txn.recoveryUnit());

                // insert three records and commit
                firstInsertResult = rs.insertRecord(&txn, "abc", 4, 1000);
                ASSERT_TRUE(firstInsertResult.isOK());

                secondInsertResult = rs.insertRecord(&txn, "123", 4, 1000);
                ASSERT_TRUE(secondInsertResult.isOK());

                thirdInsertResult = rs.insertRecord(&txn, "xyz", 4, 1000);
                ASSERT_TRUE(thirdInsertResult.isOK());

                wu.commit();

                // delete the first and third records, and commit
                rs.deleteRecord(&txn, thirdInsertResult.getValue());
                rs.deleteRecord(&txn, firstInsertResult.getValue());

                wu.commit();

                // make sure that the first and third records are gone, and that the second is still
                // there
                ASSERT_FALSE(rs.hasRecordFor(firstInsertResult.getValue()));
                ASSERT_FALSE(rs.hasRecordFor(thirdInsertResult.getValue()));
                ASSERT_TRUE(rs.hasRecordFor(secondInsertResult.getValue()));
                RecordData record = rs.dataFor(secondInsertResult.getValue());
                ASSERT_EQUALS(string("123"), string(record.data()));

                // insert a fourth record
                fourthInsertResult = rs.insertRecord(&txn, "789", 4, 1000);
                ASSERT_TRUE(fourthInsertResult.isOK());

                // update the two remaining records and commit
                firstUpdateResult = rs.updateRecord(&txn,
                                                    secondInsertResult.getValue(),
                                                    "321",
                                                    4,
                                                    1000,
                                                    NULL);


                secondUpdateResult = rs.updateRecord(&txn,
                                                     fourthInsertResult.getValue(),
                                                     "987",
                                                     4,
                                                     1000,
                                                     NULL);

                wu.commit();

            } // calls wu's destructor, equivalent to saying wu._ru->endUnitOfWork();

            // make sure that the two updates worked and persisted, and that everything is in order
            ASSERT_TRUE(firstUpdateResult.isOK());
            ASSERT_TRUE(secondUpdateResult.isOK());

            RecordData firstUpdatedRecord = rs.dataFor(firstUpdateResult.getValue());
            RecordData secondUpdatedRecord = rs.dataFor(secondUpdateResult.getValue());

            ASSERT_EQUALS(string("321"), string(firstUpdatedRecord.data()));
            ASSERT_EQUALS(string("987"), string(secondUpdatedRecord.data()));

            ASSERT_EQUALS(rs.numRecords(), 2);
        }

        // Maybe make this cleanup cleaner in the future
        Db(NULL, 0).remove(("berkeleyEnv/" + db_name + ".db").data(), NULL, 0);
        DbEnv(0).remove("berkeleyEnv/", DB_FORCE);
        remove("berkeleyEnv/log.0000000001");
    }

    TEST(BerkeleyRecordStore, MultiRollbacksMixed) {
        {
            OperationContextBerkeley txn;
            BerkeleyRecordStore rs(txn.getEnv(), db_name.data(), false, -1, -1, NULL);

            StatusWith<DiskLoc> firstInsertResult = dummyStatusWith;
            StatusWith<DiskLoc> secondInsertResult = dummyStatusWith;
            StatusWith<DiskLoc> thirdInsertResult = dummyStatusWith;

            {
                WriteUnitOfWork wu(txn.recoveryUnit());

                // insert three records and commit
                firstInsertResult = rs.insertRecord(&txn, "abc", 4, 1000);
                ASSERT_TRUE(firstInsertResult.isOK());

                secondInsertResult = rs.insertRecord(&txn, "123", 4, 1000);
                ASSERT_TRUE(secondInsertResult.isOK());

                thirdInsertResult = rs.insertRecord(&txn, "xyz", 4, 1000);
                ASSERT_TRUE(thirdInsertResult.isOK());

                wu.commit();

                // delete the first and third records, and roll it back
                rs.deleteRecord(&txn, thirdInsertResult.getValue());
                rs.deleteRecord(&txn, firstInsertResult.getValue());

            } // calls wu's destructor, equivalent to saying wu._ru->endUnitOfWork();

            // make sure everything is still there
            ASSERT_EQUALS(rs.numRecords(), 3);
            ASSERT_TRUE(rs.hasRecordFor(firstInsertResult.getValue()));
            ASSERT_TRUE(rs.hasRecordFor(thirdInsertResult.getValue()));
            ASSERT_TRUE(rs.hasRecordFor(thirdInsertResult.getValue()));

            RecordData firstRecord = rs.dataFor(firstInsertResult.getValue());
            RecordData secondRecord = rs.dataFor(secondInsertResult.getValue());
            RecordData thirdRecord = rs.dataFor(thirdInsertResult.getValue());
            ASSERT_EQUALS(string("abc"), string(firstRecord.data()));
            ASSERT_EQUALS(string("123"), string(secondRecord.data()));
            ASSERT_EQUALS(string("xyz"), string(thirdRecord.data()));

            // insert a fourth record, delete the first and third record, update the remaining two,
            // and roll it all back
            StatusWith<DiskLoc> fourthInsertResult = dummyStatusWith;
            StatusWith<DiskLoc> firstUpdateResult = dummyStatusWith; 
            StatusWith<DiskLoc> secondUpdateResult = dummyStatusWith; 

            {
                WriteUnitOfWork secondWu(txn.recoveryUnit());

                fourthInsertResult = rs.insertRecord(&txn, "789", 4, 1000);
                ASSERT_TRUE(fourthInsertResult.isOK());

                rs.deleteRecord(&txn, thirdInsertResult.getValue());
                rs.deleteRecord(&txn, firstInsertResult.getValue());

                firstUpdateResult = rs.updateRecord(&txn,
                                                    secondInsertResult.getValue(),
                                                    "321",
                                                    4,
                                                    1000,
                                                    NULL);

                secondUpdateResult = rs.updateRecord(&txn,
                                                     fourthInsertResult.getValue(),
                                                     "987",
                                                     4,
                                                     1000,
                                                     NULL);

                // before rolling back, make sure that all of these operations worked
                ASSERT_TRUE(firstUpdateResult.isOK());
                ASSERT_TRUE(secondUpdateResult.isOK());

                RecordData firstUpdatedRecord = rs.dataFor(firstUpdateResult.getValue());
                RecordData secondUpdatedRecord = rs.dataFor(secondUpdateResult.getValue());

                ASSERT_EQUALS(string("321"), string(firstUpdatedRecord.data()));
                ASSERT_EQUALS(string("987"), string(secondUpdatedRecord.data()));

                ASSERT_EQUALS(rs.numRecords(), 2);

            } // calls secondWu's destructor, equivalent to saying secondWu._ru->endUnitOfWork();

            // make sure everything was rolled back properly
            RecordData firstNewRecord = rs.dataFor(firstInsertResult.getValue());
            RecordData secondNewRecord = rs.dataFor(secondInsertResult.getValue());
            RecordData thirdNewRecord = rs.dataFor(thirdInsertResult.getValue());
            ASSERT_FALSE(rs.hasRecordFor(fourthInsertResult.getValue()));

            ASSERT_EQUALS(string("abc"), string(firstNewRecord.data()));
            ASSERT_EQUALS(string("123"), string(secondNewRecord.data()));
            ASSERT_EQUALS(string("xyz"), string(thirdNewRecord.data()));
            ASSERT_EQUALS(rs.numRecords(), 3);
        }

        // Maybe make this cleanup cleaner in the future
        Db(NULL, 0).remove(("berkeleyEnv/" + db_name + ".db").data(), NULL, 0);
        DbEnv(0).remove("berkeleyEnv/", DB_FORCE);
        remove("berkeleyEnv/log.0000000001");
    }

    TEST(BerkeleyRecordStore, MultipleRecordStores) {
        {
            OperationContextBerkeley txn;
            BerkeleyRecordStore firstRs(txn.getEnv(), db_name.data(), false, -1, -1, NULL);
            BerkeleyRecordStore secondRs(txn.getEnv(), db_name.data(), false, -1, -1, NULL);
            BerkeleyRecordStore thirdRs(txn.getEnv(), db_name.data(), false, -1, -1, NULL);

            StatusWith<DiskLoc> firstInsertResult = dummyStatusWith;
            StatusWith<DiskLoc> secondInsertResult = dummyStatusWith;
            StatusWith<DiskLoc> thirdInsertResult = dummyStatusWith;

            {
                WriteUnitOfWork wu(txn.recoveryUnit());

                firstInsertResult = firstRs.insertRecord(&txn, "abc", 4, 1000);
                secondInsertResult = secondRs.insertRecord(&txn, "abc", 4, 1000);
                thirdInsertResult = thirdRs.insertRecord(&txn, "abc", 4, 1000);

                ASSERT_TRUE(firstInsertResult.isOK());
                ASSERT_TRUE(secondInsertResult.isOK());
                ASSERT_TRUE(thirdInsertResult.isOK());
            } // calls wu's destructor, equivalent to saying wu._ru->endUnitOfWork();

            ASSERT_FALSE(firstRs.hasRecordFor(firstInsertResult.getValue()));
            ASSERT_FALSE(secondRs.hasRecordFor(secondInsertResult.getValue()));
            ASSERT_FALSE(thirdRs.hasRecordFor(thirdInsertResult.getValue()));

            ASSERT_EQUALS(firstRs.numRecords(), 0);
            ASSERT_EQUALS(secondRs.numRecords(), 0);
            ASSERT_EQUALS(thirdRs.numRecords(), 0);
        }

        // Maybe make this cleanup cleaner in the future
        Db(NULL, 0).remove(("berkeleyEnv/" + db_name + ".db").data(), NULL, 0);
        DbEnv(0).remove("berkeleyEnv/", DB_FORCE);
        remove("berkeleyEnv/log.0000000001");
    }

    TEST(BerkeleyRecordStore, Persistence1) {
        DiskLoc* loc;

        {
            OperationContextBerkeley txn;
            BerkeleyRecordStore rs(txn.getEnv(), db_name.data(), false, -1, -1, NULL);
            
            {
                WriteUnitOfWork wu(txn.recoveryUnit());
                StatusWith<DiskLoc> result = rs.insertRecord(&txn, "abc", 4, 1000);
                loc = new DiskLoc(result.getValue());

                ASSERT_TRUE(result.isOK());
                RecordData record = rs.dataFor(result.getValue());
                ASSERT_EQUALS(string("abc"), string(record.data()));

                wu.commit();
            }
        }

        {
            OperationContextBerkeley txn;
            BerkeleyRecordStore rs(txn.getEnv(), db_name.data(), false, -1, -1, NULL);
            ASSERT_EQUALS(rs.numRecords(), 1);
            {
                WriteUnitOfWork wu(txn.recoveryUnit());

                RecordData record = rs.dataFor(*loc);
                ASSERT_EQUALS(string("abc"), string(record.data()));
                ASSERT_EQUALS(rs.numRecords(), 1);

                rs.deleteRecord(&txn, *loc);
                ASSERT_FALSE(rs.hasRecordFor(*loc));
                ASSERT_EQUALS(rs.numRecords(), 0);
                ASSERT_EQUALS(rs.dataSize(), 0);

                wu.commit();
            }
        }

        {
            OperationContextBerkeley txn;
            BerkeleyRecordStore rs(txn.getEnv(), db_name.data(), false, -1, -1, NULL);
            ASSERT_EQUALS(rs.numRecords(), 0);
        }

        delete loc;

        // Maybe make this cleanup cleaner in the future
        Db(NULL, 0).remove(("berkeleyEnv/" + db_name + ".db").data(), NULL, 0);
        DbEnv(0).remove("berkeleyEnv/", DB_FORCE);
        remove("berkeleyEnv/log.0000000001");
    }

    TEST(BerkeleyRecordStore, Persistence2) {
        const int num_inserts = 1000;
        {
            OperationContextBerkeley txn;
            BerkeleyRecordStore rs(txn.getEnv(), db_name.data(), false, -1, -1, NULL);
            
            {
                WriteUnitOfWork wu(txn.recoveryUnit());
                
                for (int i = 0; i < num_inserts; i++) {
                    StatusWith<DiskLoc> result = rs.insertRecord(&txn, "abc", 4, 1000);

                    ASSERT_TRUE(result.isOK());
                    RecordData record = rs.dataFor(result.getValue());
                    ASSERT_EQUALS(string("abc"), string(record.data()));
                }

                wu.commit();
            }
        }

        {
            OperationContextBerkeley txn;
            BerkeleyRecordStore rs(txn.getEnv(), db_name.data(), false, -1, -1, NULL);
            ASSERT_EQUALS(rs.numRecords(), num_inserts);
            {
                WriteUnitOfWork wu(txn.recoveryUnit());
                
                for (int i = 0; i < num_inserts; i++) {
                    StatusWith<DiskLoc> result = rs.insertRecord(&txn, "abc", 4, 1000);

                    ASSERT_TRUE(result.isOK());
                    RecordData record = rs.dataFor(result.getValue());
                    ASSERT_EQUALS(string("abc"), string(record.data()));
                }

                wu.commit();
            }
        }

        {
            OperationContextBerkeley txn;
            BerkeleyRecordStore rs(txn.getEnv(), db_name.data(), false, -1, -1, NULL);
            ASSERT_EQUALS(rs.numRecords(), 2 * num_inserts);
        }


        // Maybe make this cleanup cleaner in the future
        Db(NULL, 0).remove(("berkeleyEnv/" + db_name + ".db").data(), NULL, 0);
        DbEnv(0).remove("berkeleyEnv/", DB_FORCE);
        remove("berkeleyEnv/log.0000000001");
    }

} // namespace
