// record_store_berkeley.cpp

/**
*    Copyright (C) 2014 MongoDB Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
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

#ifndef MONGO_BERKELEYDB
#error trying to compile record_store_berkeley.cpp with MONGO_BERKELEYDB undefined
#endif

#include "mongo/db/storage/berkeley1/record_store_berkeley.h"

#include "mongo/db/operation_context.h"
#include "mongo/db/storage/berkeley1/berkeley1_recovery_unit.h"
#include "mongo/db/storage/berkeley1/mongo_bdb.h"


//TODO find maximum record length
#define BUFFER_SIZE 16 * 1024000

namespace mongo {

    //
    // RecordStore
    //

    BerkeleyRecordStore::BerkeleyRecordStore(DbEnv& env,
                                     const StringData& ns,
                                     bool isCapped,
                                     int64_t cappedMaxSize,
                                     int64_t cappedMaxDocs,
                                     CappedDocumentDeleteCallback* cappedDeleteCallback)
            : RecordStore(ns),
              _isCapped(isCapped),
              _cappedMaxSize(cappedMaxSize),
              _cappedMaxDocs(cappedMaxDocs),
              _cappedDeleteCallback(cappedDeleteCallback),
              db(&env, 0),
              _env(env) { // DiskLoc(0,0) isn't valid for records.

        if (_isCapped) {
            invariant(_cappedMaxSize > 0);
            invariant(_cappedMaxDocs == -1 || _cappedMaxDocs > 0);
        }
        else {
            invariant(_cappedMaxSize == -1);
            invariant(_cappedMaxDocs == -1);
        }

        // Open DB
        try {
              //TODO set error stream
              //db.set_error_stream(error());

              uint32_t cFlags_ = (DB_CREATE | DB_AUTO_COMMIT | DB_READ_UNCOMMITTED);
              // Open the database
              std::string db_name = ns.toString() + ".db";
              db.open(NULL, db_name.data(), NULL, DB_BTREE, cFlags_, 0);
          }
          // DbException is not a subclass of std::exception, so we
          // need to catch them both.
          catch(DbException &e) {
              error() << "Error opening database: " << ns.toString() << "\n";
              error() << e.what() << std::endl;
          }
          catch(std::exception &e) {
              error() << "Error opening database: " << ns.toString() << "\n";
              error() << e.what() << std::endl;
          }
        // Initialize Read Buffer
        readBuffer = boost::shared_array<char>(new char[BUFFER_SIZE]);
    }

    BerkeleyRecordStore::~BerkeleyRecordStore() {
        db.close(0);
    }

    const char* BerkeleyRecordStore::name() const { return "berkeley"; }


    RecordData BerkeleyRecordStore::dataFor(const DiskLoc& loc) const {
        Dbt value;

        int64_t key_id = getLocID(loc);
        Dbt key(reinterpret_cast<char *>(&key_id), sizeof(int64_t));
        
        value.set_data(readBuffer.get());
        value.set_flags(DB_DBT_USERMEM);
        value.set_ulen(BUFFER_SIZE);

        invariant(const_cast<Db&>(db).get(NULL, &key, &value, DB_READ_UNCOMMITTED) == 0);

        int size = value.get_size();

        char* data = new char[size];
        memcpy(data, readBuffer.get(), size);

        return RecordData(data, size, boost::shared_array<char>(data));
    }

    void BerkeleyRecordStore::deleteRecord(OperationContext* txn, const DiskLoc& loc)  {
        int64_t key_id = getLocID(loc);
        Dbt key(reinterpret_cast<char *>(&key_id), sizeof(int64_t));

        DbTxn* ru = reinterpret_cast<Berkeley1RecoveryUnit*>(txn->recoveryUnit())->
                          getCurrentTransaction();
        //invariant(ru != NULL);

        invariant(db.del(ru, &key, 0) == 0);    }

    bool BerkeleyRecordStore::cappedAndNeedDelete() const {
        invariant(!"nyi");
    }

    void BerkeleyRecordStore::cappedDeleteAsNeeded(OperationContext* txn) {
        invariant(!"nyi");
    }

    StatusWith<DiskLoc> BerkeleyRecordStore::insertRecord(OperationContext* txn,
                                                      const char* data,
                                                      int len,
                                                      int quotaMax) {
        Dbt value;

        if (_isCapped && len > _cappedMaxSize) {
            // We use dataSize for capped rollover and we don't want to delete everything if we know
            // this won't fit.
            return StatusWith<DiskLoc>(ErrorCodes::BadValue,
                                       "object to insert exceeds cappedMaxSize");
        }

        const DiskLoc loc = allocateLoc(txn);
        
        value.set_size(len);
        value.set_data(const_cast<char*>(data));
        int64_t key_id = getLocID(loc);
        Dbt key(reinterpret_cast<char *>(&key_id), sizeof(int64_t));


        DbTxn* ru = reinterpret_cast<Berkeley1RecoveryUnit*>(txn->recoveryUnit())->
                          getCurrentTransaction();
        //invariant(ru != NULL);

        invariant(db.put(ru, &key, &value, DB_NOOVERWRITE) == 0);

        return StatusWith<DiskLoc>(loc);
    }

    StatusWith<DiskLoc> BerkeleyRecordStore::insertRecord(OperationContext* txn,
                                                      const DocWriter* doc,
                                                      int quotaMax) {
        Dbt value;

        const int len = doc->documentSize();
        if (_isCapped && len > _cappedMaxSize) {
            // We use dataSize for capped rollover and we don't want to delete everything if we know
            // this won't fit.
            return StatusWith<DiskLoc>(ErrorCodes::BadValue,
                                       "object to insert exceeds cappedMaxSize");
        }

        boost::shared_array<char> buf(new char[len]);
        doc->writeDocument(buf.get());

        value.set_size(len);
        value.set_data(buf.get());

        const DiskLoc loc = allocateLoc(txn);

        int64_t key_id = getLocID(loc);
        Dbt key(reinterpret_cast<char *>(&key_id), sizeof(int64_t));

        DbTxn* ru = reinterpret_cast<Berkeley1RecoveryUnit*>(txn->recoveryUnit())->
                          getCurrentTransaction();
        //invariant(ru != NULL);

        invariant(db.put(ru, &key, &value, DB_NOOVERWRITE) == 0);

        return StatusWith<DiskLoc>(loc);
    }

    StatusWith<DiskLoc> BerkeleyRecordStore::updateRecord(OperationContext* txn,
                                                      const DiskLoc& oldLocation,
                                                      const char* data,
                                                      int len,
                                                      int quotaMax,
                                                      UpdateMoveNotifier* notifier ) {
        Dbt value;

        if (_isCapped && len > _cappedMaxSize) {
            // We use dataSize for capped rollover and we don't want to delete everything if we know
            // this won't fit.
            return StatusWith<DiskLoc>(ErrorCodes::BadValue,
                                       "object to insert exceeds cappedMaxSize");
        }

        value.set_size(len);
        value.set_data(const_cast<char*>(data));
        int64_t key_id = getLocID(oldLocation);
        Dbt key(reinterpret_cast<char *>(&key_id), sizeof(int64_t));

        DbTxn* ru = reinterpret_cast<Berkeley1RecoveryUnit*>(txn->recoveryUnit())->
                          getCurrentTransaction();

        invariant(ru != NULL);         

        invariant(db.put(ru, &key, &value, 0) == 0);

        return StatusWith<DiskLoc>(oldLocation);
    }

    Status BerkeleyRecordStore::updateWithDamages( OperationContext* txn,
                                               const DiskLoc& loc,
                                               const char* damangeSource,
                                               const mutablebson::DamageVector& damages ) {
        invariant(!"updateRecord not yet implemented");
    }

    RecordIterator* BerkeleyRecordStore::getIterator(const DiskLoc& start,
                                                 bool tailable,
                                                 const CollectionScanParams::Direction& dir) const {
        invariant(!"nyi");
    }

    RecordIterator* BerkeleyRecordStore::getIteratorForRepair() const {
        invariant(!"nyi");
    }

    std::vector<RecordIterator*> BerkeleyRecordStore::getManyIterators() const {
        invariant(!"nyi");
    }


    Status BerkeleyRecordStore::truncate(OperationContext* txn) {
        db.truncate(reinterpret_cast<Berkeley1RecoveryUnit*>(txn->recoveryUnit())->
                          getCurrentTransaction(), NULL, 0);

        return Status::OK();
    }

    bool BerkeleyRecordStore::compactSupported() const {
        return true;
    }
    Status BerkeleyRecordStore::compact(OperationContext* txn,
                                    RecordStoreCompactAdaptor* adaptor,
                                    const CompactOptions* options,
                                    CompactStats* stats) {
        invariant(db.compact(reinterpret_cast<Berkeley1RecoveryUnit*>(txn->recoveryUnit())->
                          getCurrentTransaction(), NULL, NULL, NULL, 0, NULL) == 0);

        return Status::OK();
    }

    Status BerkeleyRecordStore::validate(OperationContext* txn,
                                     bool full,
                                     bool scanData,
                                     ValidateAdaptor* adaptor,
                                     ValidateResults* results,
                                     BSONObjBuilder* output) const {
        invariant(!"nyi");
    }

    void BerkeleyRecordStore::appendCustomStats( BSONObjBuilder* result, double scale ) const {
        invariant(!"appendCustomStats not yet implemented");
    }

    Status BerkeleyRecordStore::touch(OperationContext* txn, BSONObjBuilder* output) const {
        invariant(!"nyi");
    }

    Status BerkeleyRecordStore::setCustomOption(
                OperationContext* txn, const BSONElement& option, BSONObjBuilder* info) {
        invariant(!"setCustomOption not yet implemented");
    }

    void BerkeleyRecordStore::increaseStorageSize(OperationContext* txn,  int size, int quotaMax) {
        // unclear what this would mean for this class. For now, just no-op.
    }

    int64_t BerkeleyRecordStore::storageSize(BSONObjBuilder* extraInfo, int infoLevel) const {
        // Note: not making use of extraInfo or infoLevel since we don't have extents
        invariant(!"nyi");
    }


    bool BerkeleyRecordStore::hasRecordFor(const DiskLoc& loc) const {
        int64_t key_id = getLocID(loc);
        Dbt key(reinterpret_cast<char *>(&key_id), sizeof(int64_t));
        
        return (const_cast<Db&>(db).exists(NULL, &key, DB_READ_UNCOMMITTED) != DB_NOTFOUND);
    }

    DiskLoc BerkeleyRecordStore::allocateLoc(OperationContext* txn) {
        char id_key = 0;
        int64_t id = 0, new_id = 0;

        Dbt key(reinterpret_cast<char*>(&id_key), sizeof(char));
        Dbt value(reinterpret_cast<char*>(&id), sizeof(int64_t));
        value.set_flags(DB_DBT_USERMEM);
        value.set_ulen(sizeof(int64_t));

        DbTxn* transaction = NULL;
        _env.txn_begin(reinterpret_cast<Berkeley1RecoveryUnit*>(txn->recoveryUnit())->
                          getCurrentTransaction(), &transaction, 0);
        if (const_cast<Db&>(db).get(transaction, &key, &value, DB_READ_UNCOMMITTED) == DB_NOTFOUND) {
          id = 0;
        }

        new_id = id + 1;
        Dbt new_value(reinterpret_cast<char*>(&new_id), sizeof(int64_t));

        invariant(db.put(transaction, &key, &new_value, 0) == 0);
        transaction->commit(0);

        // This is a hack, but both the high and low order bits of DiskLoc offset must be 0, and the
        // file must fit in 23 bits. This gives us a total of 30 + 23 == 53 bits.
        invariant(id < (1LL << 53));
        return DiskLoc(int(id >> 30), int((id << 1) & ~(1<<31)));
    }

    int64_t BerkeleyRecordStore::getLocID(const DiskLoc& loc) const {
        return (((int64_t) loc.getOfs() << 32) + (int64_t) loc.a());
    }

    long long BerkeleyRecordStore::dataSize() const {
        DB_BTREE_STAT* stat = NULL;
        stat = (DB_BTREE_STAT*) malloc(sizeof(DB_BTREE_STAT));

        invariant(const_cast<Db&>(db).stat(NULL, &stat, DB_READ_UNCOMMITTED) == 0);
        long long num_records = (long long) stat->bt_nkeys;

        // This is to account for the one record which is used to allocate the next location
        if (num_records != 0)
          num_records--;
        return num_records * (long long)stat->bt_pagesize;
    }

    long long BerkeleyRecordStore::numRecords() const {
        DB_BTREE_STAT* stat = NULL;
        stat = (DB_BTREE_STAT*) malloc(sizeof(DB_BTREE_STAT));

        invariant(const_cast<Db&>(db).stat(NULL, &stat, DB_READ_UNCOMMITTED) == 0);
        long long num_records = (long long) stat->bt_nkeys;

        // This is to account for the one record which is used to allocate the next location
        if (num_records != 0)
          num_records--;
        return num_records;
    }


    //
    // Forward Iterator
    //

    BerkeleyRecordIterator::BerkeleyRecordIterator(const BerkeleyRecordStore::Records& records,
                                           const BerkeleyRecordStore& rs,
                                           DiskLoc start,
                                           bool tailable)
            : _tailable(tailable),
              _killedByInvalidate(false),
              _records(records),
              _rs(rs) {
        if (start.isNull()) {
            _it = _records.begin();
        }
        else {
            _it = _records.find(start);
            invariant(_it != _records.end());
        }
    }

    bool BerkeleyRecordIterator::isEOF() {
        invariant(!"nyi");
    }

    DiskLoc BerkeleyRecordIterator::curr() {
        invariant(!"nyi");
    }

    DiskLoc BerkeleyRecordIterator::getNext() {
        invariant(!"nyi");
    }

    void BerkeleyRecordIterator::invalidate(const DiskLoc& loc) {
        invariant(!"nyi");
    }

    void BerkeleyRecordIterator::prepareToYield() {
        invariant(!"nyi");
    }

    bool BerkeleyRecordIterator::recoverFromYield() {
        invariant(!"nyi");
    }

    const Record* BerkeleyRecordIterator::recordFor(const DiskLoc& loc) const {
        invariant(!"nyi");
    }

    //
    // Reverse Iterator
    //

    BerkeleyRecordReverseIterator::BerkeleyRecordReverseIterator(const BerkeleyRecordStore::Records& records,
                                                         const BerkeleyRecordStore& rs,
                                                         DiskLoc start)
            : _killedByInvalidate(false),
              _records(records),
              _rs(rs) {
        invariant(!"nyi");
    }

    bool BerkeleyRecordReverseIterator::isEOF() {
        invariant(!"nyi");
    }

    DiskLoc BerkeleyRecordReverseIterator::curr() {
        invariant(!"nyi");
    }

    DiskLoc BerkeleyRecordReverseIterator::getNext() {
        invariant(!"nyi");
    }

    void BerkeleyRecordReverseIterator::invalidate(const DiskLoc& loc) {
        invariant(!"nyi");
    }

    void BerkeleyRecordReverseIterator::prepareToYield() {
    }

    bool BerkeleyRecordReverseIterator::recoverFromYield() {
        invariant(!"nyi");
    }

    const Record* BerkeleyRecordReverseIterator::recordFor(const DiskLoc& loc) const {
        invariant(!"nyi");
    }
} // namespace mongo
