// record_store_twitter.cpp

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

#include "mongo/db/storage/twitter/twitter_record_store.h"

#include "mongo/db/operation_context_noop.h"

namespace mongo {

    //
    // RecordStore
    //

    TwitterRecordStore::TwitterRecordStore(const StringData& ns,
                                     bool isCapped,
                                     int64_t cappedMaxSize,
                                     int64_t cappedMaxDocs,
                                     CappedDocumentDeleteCallback* cappedDeleteCallback)
                                     : RecordStore(ns) {
        invariant(!"nyi");
    }

    const char* TwitterRecordStore::name() const { return "twitter"; }

    RecordData TwitterRecordStore::dataFor( const DiskLoc& loc ) const {
        invariant(!"nyi");
    }

    TwitterRecordStore::TwitterRecord* TwitterRecordStore::recordFor(const DiskLoc& loc) const {
        invariant(!"nyi");
    }

    void TwitterRecordStore::deleteRecord(OperationContext* txn, const DiskLoc& loc) {
        invariant(!"nyi");
    }

    StatusWith<DiskLoc> TwitterRecordStore::insertRecord(OperationContext* txn,
                                                      const char* data,
                                                      int len,
                                                      bool enforceQuota) {
        invariant(!"nyi");
    }

    StatusWith<DiskLoc> TwitterRecordStore::insertRecord(OperationContext* txn,
                                                      const DocWriter* doc,
                                                      bool enforceQuota) {
        invariant(!"nyi");
    }

    StatusWith<DiskLoc> TwitterRecordStore::updateRecord(OperationContext* txn,
                                                      const DiskLoc& oldLocation,
                                                      const char* data,
                                                      int len,
                                                      bool enforceQuota,
                                                      UpdateMoveNotifier* notifier ) {
        invariant(!"nyi");
    }

    Status TwitterRecordStore::updateWithDamages( OperationContext* txn,
                                               const DiskLoc& loc,
                                               const char* damangeSource,
                                               const mutablebson::DamageVector& damages ) {
        invariant(!"nyi");
    }

    RecordIterator* TwitterRecordStore::getIterator(OperationContext* txn,
                                                 const DiskLoc& start,
                                                 bool tailable,
                                                 const CollectionScanParams::Direction& dir) const {
        invariant(!"nyi");
    }

    RecordIterator* TwitterRecordStore::getIteratorForRepair(OperationContext* txn) const {
        invariant(!"nyi");
    }

    std::vector<RecordIterator*> TwitterRecordStore::getManyIterators(OperationContext* txn) const {
        invariant(!"nyi");
    }

    Status TwitterRecordStore::truncate(OperationContext* txn) {
        invariant(!"nyi");
    }

    void TwitterRecordStore::temp_cappedTruncateAfter(OperationContext* txn,
                                                   DiskLoc end,
                                                   bool inclusive) {
        invariant(!"nyi");
    }

    bool TwitterRecordStore::compactSupported() const {
        invariant(!"nyi");
    }

    Status TwitterRecordStore::compact(OperationContext* txn,
                                    RecordStoreCompactAdaptor* adaptor,
                                    const CompactOptions* options,
                                    CompactStats* stats) {
        invariant(!"nyi");
    }

    Status TwitterRecordStore::validate(OperationContext* txn,
                                     bool full,
                                     bool scanData,
                                     ValidateAdaptor* adaptor,
                                     ValidateResults* results,
                                     BSONObjBuilder* output) const {
        invariant(!"nyi");
    }
    
    void TwitterRecordStore::appendCustomStats( OperationContext* txn,
                                             BSONObjBuilder* result,
                                             double scale ) const {
        invariant(!"nyi");
    }

    Status TwitterRecordStore::touch(OperationContext* txn, BSONObjBuilder* output) const {
        invariant(!"nyi");
    }
    
    Status TwitterRecordStore::setCustomOption(
                OperationContext* txn, const BSONElement& option, BSONObjBuilder* info) {
        invariant(!"setCustomOption not yet implemented");
    }

    void TwitterRecordStore::increaseStorageSize(OperationContext* txn,  int size, bool enforceQuota) {
        // unclear what this would mean for this class. For now, just error if called.
        invariant(!"increaseStorageSize not yet implemented");
    }

    int64_t TwitterRecordStore::storageSize(OperationContext* txn,
                                         BSONObjBuilder* extraInfo,
                                         int infoLevel) const {
        invariant(!"nyi");
    }

    //
    // Forward Iterator
    //

    TwitterRecordIterator::TwitterRecordIterator(OperationContext* txn,
                                           const TwitterRecordStore::Records& records,
                                           const TwitterRecordStore& rs,
                                           DiskLoc start,
                                           bool tailable) {
        invariant(!"nyi");
    }

    bool TwitterRecordIterator::isEOF() {
        invariant(!"nyi");
    }

    DiskLoc TwitterRecordIterator::curr() {
        invariant(!"nyi");
    }

    DiskLoc TwitterRecordIterator::getNext() {
        invariant(!"nyi");
    }

    void TwitterRecordIterator::invalidate(const DiskLoc& loc) {
        invariant(!"nyi");
    }

    void TwitterRecordIterator::prepareToYield() {
        invariant(!"nyi");
    }

    bool TwitterRecordIterator::recoverFromYield() {
        invariant(!"nyi");
    }

    RecordData TwitterRecordIterator::dataFor(const DiskLoc& loc) const {
        invariant(!"nyi");
    }

    //
    // Reverse Iterator
    //

    TwitterRecordReverseIterator::TwitterRecordReverseIterator(OperationContext* txn,
                                                         const TwitterRecordStore::Records& records,
                                                         const TwitterRecordStore& rs,
                                                         DiskLoc start) {
        invariant(!"nyi");
    }

    bool TwitterRecordReverseIterator::isEOF() {
        invariant(!"nyi");
    }

    DiskLoc TwitterRecordReverseIterator::curr() {
        invariant(!"nyi");
    }

    DiskLoc TwitterRecordReverseIterator::getNext() {
        invariant(!"nyi");
    }

    void TwitterRecordReverseIterator::invalidate(const DiskLoc& loc) {
        invariant(!"nyi");
    }

    void TwitterRecordReverseIterator::prepareToYield() {
        invariant(!"nyi");
    }

    bool TwitterRecordReverseIterator::recoverFromYield() {
        invariant(!"nyi");
    }

    RecordData TwitterRecordReverseIterator::dataFor(const DiskLoc& loc) const {
        invariant(!"nyi");
    }
} // namespace mongo
