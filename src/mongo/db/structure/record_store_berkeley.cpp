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

#include "mongo/db/structure/record_store_berkeley.h"

#include <db_cxx.h>

#include "mongo/db/storage/record.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/storage/berkeley/berkeley_recovery_unit.h"

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
              _dataSize(0),
              _nextId(1),
              _numRecords(0),
              _cappedDeleteCallback(cappedDeleteCallback),
              db(&env, 0) { // DiskLoc(0,0) isn't valid for records.

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

              uint32_t cFlags_ = (DB_CREATE | DB_AUTO_COMMIT);
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
          //TODO find maximum record length
        readBuffer = boost::shared_array<char>(new char[16 * 1024000]);
    }

    const char* BerkeleyRecordStore::name() const { return "berkeley"; }

    Record* BerkeleyRecordStore::recordFor(const DiskLoc& loc) const {
        Dbt value;

        int64_t key_id = getLocID(loc);
        // TODO make sure this copies into the Dbt
        Dbt key(reinterpret_cast<char *>(&key_id), sizeof(int64_t));
        
        value.set_data(readBuffer.get());
        value.set_flags(DB_DBT_USERMEM);

        invariant(const_cast<Db&>(db).get(NULL, &key, &value, 0) == 0);

        int size = reinterpret_cast<Record*>(readBuffer.get())->lengthWithHeaders();

        boost::shared_array<char> rec(new char[size]);
        memcpy(rec.get(), readBuffer.get(), size);

        return reinterpret_cast<Record*>(rec.get());
        //TODO change!
    }

    void BerkeleyRecordStore::deleteRecord(OperationContext* txn, const DiskLoc& loc)  {
        int64_t key_id = getLocID(loc);
        // TODO make sure this copies into the Dbt
        Dbt key(reinterpret_cast<char *>(&key_id), sizeof(int64_t));

        invariant(db.del(reinterpret_cast<BerkeleyRecoveryUnit*>(txn->recoveryUnit())->
                          getCurrentTransaction(), &key, 0) == 0);
    }

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

        // TODO padding?
        const int lengthWithHeaders = len + Record::HeaderSize;
        boost::shared_array<char> buf(new char[lengthWithHeaders]);
        Record* rec = reinterpret_cast<Record*>(buf.get());
        rec->lengthWithHeaders() = lengthWithHeaders;
        memcpy(rec->data(), data, len);

        const DiskLoc loc = allocateLoc();
        
        value.set_size(lengthWithHeaders);
        value.set_data(rec);

        int64_t key_id = getLocID(loc);
        // TODO make sure this copies into the Dbt
        Dbt key(reinterpret_cast<char *>(&key_id), sizeof(int64_t));

        invariant(db.put(reinterpret_cast<BerkeleyRecoveryUnit*>(txn->recoveryUnit())->
                          getCurrentTransaction(), &key, &value, DB_NOOVERWRITE) == 0);

        _dataSize += len;

        cappedDeleteAsNeeded(txn);

        return StatusWith<DiskLoc>(loc);
    }

    StatusWith<DiskLoc> BerkeleyRecordStore::insertRecord(OperationContext* txn,
                                                      const DocWriter* doc,
                                                      int quotaMax) {
        invariant(!"nyi");
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

        Record* oldRecord = recordFor(oldLocation);

        const int lengthWithHeaders = len + Record::HeaderSize;
        boost::shared_array<char> buf(new char[lengthWithHeaders]);
        Record* rec = reinterpret_cast<Record*>(buf.get());
        rec->lengthWithHeaders() = lengthWithHeaders;
        memcpy(rec->data(), data, len);

        
        value.set_size(lengthWithHeaders);
        value.set_data(rec);

        int64_t key_id = getLocID(oldLocation);
        // TODO make sure this copies into the Dbt
        Dbt key(reinterpret_cast<char *>(&key_id), sizeof(int64_t));

        invariant(db.put(reinterpret_cast<BerkeleyRecoveryUnit*>(txn->recoveryUnit())->
                          getCurrentTransaction(), &key, &value, 0) == 0);

        _dataSize += len - oldRecord->netLength();

        cappedDeleteAsNeeded(txn);

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
        invariant(!"nyi");
    }

    bool BerkeleyRecordStore::compactSupported() const {
        invariant(!"nyi");
    }
    Status BerkeleyRecordStore::compact(OperationContext* txn,
                                    RecordStoreCompactAdaptor* adaptor,
                                    const CompactOptions* options,
                                    CompactStats* stats) {
        // TODO might be possible to do something here
        invariant(!"compact not yet implemented");
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
        invariant(!"nyi");
    }

    int64_t BerkeleyRecordStore::storageSize(BSONObjBuilder* extraInfo, int infoLevel) const {
        // Note: not making use of extraInfo or infoLevel since we don't have extents
        invariant(!"nyi");
    }


    bool BerkeleyRecordStore::hasRecordFor(const DiskLoc& loc) const {
        invariant(!"nyi");
    }

    DiskLoc BerkeleyRecordStore::allocateLoc() {
        const int64_t id = _nextId++;
        // This is a hack, but both the high and low order bits of DiskLoc offset must be 0, and the
        // file must fit in 23 bits. This gives us a total of 30 + 23 == 53 bits.
        invariant(id < (1LL << 53));
        return DiskLoc(int(id >> 30), int((id << 1) & ~(1<<31)));
    }

    int64_t BerkeleyRecordStore::getLocID(const DiskLoc& loc) const {
        return (((int64_t) loc.getOfs() << 32) + (int64_t) loc.a());
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
