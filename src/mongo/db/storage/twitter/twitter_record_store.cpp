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
#include <twitcurl/twitcurl.h>

namespace mongo {

    //
    // RecordStore
    //

    TwitterRecordStore::TwitterRecordStore(TwitterCUD& tcud, HeapRecordStore* hrs,
                                      const StringData& ns)
                                     : RecordStore(ns),
                                     _hrs(hrs),
                                     _tcud(tcud) {
    }

    const char* TwitterRecordStore::name() const { return "twitter"; }

    RecordData TwitterRecordStore::dataFor( const DiskLoc& loc ) const {
        return _hrs->dataFor(loc);
    }

    void TwitterRecordStore::deleteRecord(OperationContext* txn, const DiskLoc& loc) {
        if (_tcud.remove(loc, _ns))
          _hrs->deleteRecord(txn, loc);
    }

    StatusWith<DiskLoc> TwitterRecordStore::insertRecord(OperationContext* txn,
                                                      const char* data,
                                                      int len,
                                                      bool enforceQuota) {
        BSONObj obj(data);

        StatusWith<DiskLoc> loc = _hrs->insertRecord(txn, data, len, enforceQuota);
        if (!loc.isOK())
            return loc;

        if (_tcud.insert(obj, loc.getValue(), _ns)) {
            return loc;
        }

        _hrs->deleteRecord(txn, loc.getValue());
        return StatusWith<DiskLoc>(ErrorCodes::BadValue,
                                   "Twitter Reject");
    }

    StatusWith<DiskLoc> TwitterRecordStore::insertRecord(OperationContext* txn,
                                                      const DocWriter* doc,
                                                      bool enforceQuota) {
        boost::shared_array<char> buf(new char[doc->documentSize()]);
        doc->writeDocument(buf.get());

        BSONObj obj(buf.get());

        StatusWith<DiskLoc> loc = _hrs->insertRecord(txn, doc, enforceQuota);
        if (!loc.isOK())
            return loc;

        if (_tcud.insert(obj, loc.getValue(), _ns)) {
            return loc;
        }

        _hrs->deleteRecord(txn, loc.getValue());
        return StatusWith<DiskLoc>(ErrorCodes::BadValue,
                                   "Twitter Reject");
    }

    StatusWith<DiskLoc> TwitterRecordStore::updateRecord(OperationContext* txn,
                                                      const DiskLoc& oldLocation,
                                                      const char* data,
                                                      int len,
                                                      bool enforceQuota,
                                                      UpdateMoveNotifier* notifier ) {
        BSONObj obj(data);
        if (_tcud.insert(obj, oldLocation, _ns))
            // this assumes heap never changes DiskLoc
            return _hrs->updateRecord(txn, oldLocation, data, len, enforceQuota, notifier );

        return StatusWith<DiskLoc>(ErrorCodes::BadValue,
                                   "Twitter Reject");
    }

    Status TwitterRecordStore::updateWithDamages( OperationContext* txn,
                                               const DiskLoc& loc,
                                               const char* damangeSource,
                                               const mutablebson::DamageVector& damages ) {
        RecordData oldData = _hrs->dataFor(loc);
        boost::shared_array<char> save(new char[oldData.size()]);
        memcpy(save.get(), oldData.data(), oldData.size());

        Status s = _hrs->updateWithDamages( txn, loc, damangeSource, damages );
        if (!s.isOK())
            return s;

        RecordData data = _hrs->dataFor(loc);

        BSONObj obj(data.data());

        if (_tcud.insert(obj, loc, _ns)) {
            return s;
        }

        _hrs->updateRecord(txn, loc, save.get(), oldData.size(), false, NULL );
        return Status(ErrorCodes::BadValue,
                                   "Twitter Reject");

    }

    RecordIterator* TwitterRecordStore::getIterator(OperationContext* txn,
                                                 const DiskLoc& start,
                                                 bool tailable,
                                                 const CollectionScanParams::Direction& dir) const {
        return _hrs->getIterator(txn, start, tailable, dir);
    }

    RecordIterator* TwitterRecordStore::getIteratorForRepair(OperationContext* txn) const {
        return _hrs->getIteratorForRepair(txn);
    }

    std::vector<RecordIterator*> TwitterRecordStore::getManyIterators(OperationContext* txn) const {
        return _hrs->getManyIterators(txn);
    }

    Status TwitterRecordStore::truncate(OperationContext* txn) {
        if (_tcud.custom("truncate", _ns))
            return _hrs->truncate(txn);
        else 
            return Status(ErrorCodes::BadValue,
                                   "Twitter Reject");
    }

    void TwitterRecordStore::temp_cappedTruncateAfter(OperationContext* txn,
                                                   DiskLoc end,
                                                   bool inclusive) {
        // Noop
    }

    bool TwitterRecordStore::compactSupported() const {
        return false;
    }

    Status TwitterRecordStore::compact(OperationContext* txn,
                                    RecordStoreCompactAdaptor* adaptor,
                                    const CompactOptions* options,
                                    CompactStats* stats) {
        return Status::OK();
    }

    Status TwitterRecordStore::validate(OperationContext* txn,
                                     bool full,
                                     bool scanData,
                                     ValidateAdaptor* adaptor,
                                     ValidateResults* results,
                                     BSONObjBuilder* output) const {
        return Status::OK();
    }
    
    void TwitterRecordStore::appendCustomStats( OperationContext* txn,
                                             BSONObjBuilder* result,
                                             double scale ) const {
        // noop
    }

    Status TwitterRecordStore::touch(OperationContext* txn, BSONObjBuilder* output) const {
        // noop
      return Status::OK();
    }
    
    Status TwitterRecordStore::setCustomOption(
                OperationContext* txn, const BSONElement& option, BSONObjBuilder* info) {
        return Status::OK();
    }

    void TwitterRecordStore::increaseStorageSize(OperationContext* txn,  int size, bool enforceQuota) {
        // noop
    }

    int64_t TwitterRecordStore::storageSize(OperationContext* txn,
                                         BSONObjBuilder* extraInfo,
                                         int infoLevel) const {
        return _hrs->storageSize(txn, extraInfo, infoLevel);
    }
} // namespace mongo
