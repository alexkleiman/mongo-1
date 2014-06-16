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

    BerkeleyRecordStore::BerkeleyRecordStore(const StringData& ns,
                                     bool isCapped,
                                     int64_t cappedMaxSize,
                                     int64_t cappedMaxDocs,
                                     CappedDocumentDeleteCallback* cappedDeleteCallback,
                                     DbEnv env)
            : RecordStore(ns),
              _isCapped(isCapped),
              _cappedMaxSize(cappedMaxSize),
              _cappedMaxDocs(cappedMaxDocs),
              _cappedDeleteCallback(cappedDeleteCallback),
              _dataSize(0),
              _nextId(1) { // DiskLoc(0,0) isn't valid for records.

        if (_isCapped) {
            invariant(_cappedMaxSize > 0);
            invariant(_cappedMaxDocs == -1 || _cappedMaxDocs > 0);
        }
        else {
            invariant(_cappedMaxSize == -1);
            invariant(_cappedMaxDocs == -1);
        }

        // Open DB
        try
            {
                db = new Db(env, 0);
                db_.set_error_stream(error());

                uint32_t cFlags_ = (DB_CREATE | DB_AUTO_COMMIT);
                // Open the database
                db.open(NULL, ns.toString() + ".db", NULL, DB_BTREE, cFlags_, 0);
            }
            // DbException is not a subclass of std::exception, so we
            // need to catch them both.
            catch(DbException &e) 
            {
                error() << "Error opening database: " << ns.toString() << "\n";
                error() << e.what() << std::endl;
            }
            catch(std::exception &e)
            {
                error() << "Error opening database: " << ns.toString() << "\n";
                error() << e.what() << std::endl;
            }
    }

    const char* BerkeleyRecordStore::name() const { return "berkeley"; }

    Record* BerkeleyRecordStore::recordFor(const DiskLoc& loc) const {
        invariant(!"nyi");
    }

    void BerkeleyRecordStore::deleteRecord(OperationContext* txn, const DiskLoc& loc) {
        invariant(!"nyi");
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
        invariant(!"nyi");
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
        invariant(!"nyi");
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
        invariant(!"nyi");
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
