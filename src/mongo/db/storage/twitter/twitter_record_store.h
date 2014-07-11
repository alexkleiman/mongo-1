// record_store_twitter.h

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

#pragma once

#include <boost/shared_array.hpp>
#include <map>

#include "mongo/db/structure/capped_callback.h"
#include "mongo/db/structure/record_store.h"
#include "mongo/db/structure/record_store_heap.h"
#include "mongo/db/storage/twitter/twitter_cud.h"

namespace mongo {
    class TwitterRecordIterator;

    /**
     * A RecordStore that stores all data on the twitter.
     *
     * @param cappedMaxSize - required if isCapped. limit uses dataSize() in this impl.
     */
    class TwitterRecordStore : public RecordStore {
    public:
        explicit TwitterRecordStore(TwitterCUD& tcud,
                                 HeapRecordStore* hrs,
                                 const StringData& ns);

        virtual const char* name() const;

        virtual RecordData dataFor( const DiskLoc& loc ) const;

        virtual void deleteRecord( OperationContext* txn, const DiskLoc& dl );

        virtual StatusWith<DiskLoc> insertRecord( OperationContext* txn,
                                                  const char* data,
                                                  int len,
                                                  bool enforceQuota );

        virtual StatusWith<DiskLoc> insertRecord( OperationContext* txn,
                                                  const DocWriter* doc,
                                                  bool enforceQuota );
                                                  
        virtual StatusWith<DiskLoc> updateRecord( OperationContext* txn,
                                                  const DiskLoc& oldLocation,
                                                  const char* data,
                                                  int len,
                                                  bool enforceQuota,
                                                  UpdateMoveNotifier* notifier );
                                                  
        virtual Status updateWithDamages( OperationContext* txn,
                                          const DiskLoc& loc,
                                          const char* damangeSource,
                                          const mutablebson::DamageVector& damages );

        virtual RecordIterator* getIterator( OperationContext* txn,
                                             const DiskLoc& start,
                                             bool tailable,
                                             const CollectionScanParams::Direction& dir) const;

        virtual RecordIterator* getIteratorForRepair( OperationContext* txn ) const;

        virtual std::vector<RecordIterator*> getManyIterators( OperationContext* txn ) const;

        virtual Status truncate( OperationContext* txn );

        virtual void temp_cappedTruncateAfter( OperationContext* txn, DiskLoc end, bool inclusive );

        virtual bool compactSupported() const;
        virtual Status compact( OperationContext* txn,
                                RecordStoreCompactAdaptor* adaptor,
                                const CompactOptions* options,
                                CompactStats* stats );

        virtual Status validate( OperationContext* txn,
                                 bool full,
                                 bool scanData,
                                 ValidateAdaptor* adaptor,
                                 ValidateResults* results, BSONObjBuilder* output ) const;
                                 
        virtual void appendCustomStats( OperationContext* txn,
                                        BSONObjBuilder* result,
                                        double scale ) const;

        virtual Status touch( OperationContext* txn, BSONObjBuilder* output ) const;
        
        virtual Status setCustomOption( OperationContext* txn,
                                        const BSONElement& option,
                                        BSONObjBuilder* info = NULL );

        virtual void increaseStorageSize( OperationContext* txn,  int size, bool enforceQuota );

        virtual int64_t storageSize( OperationContext* txn,
                                     BSONObjBuilder* extraInfo = NULL,
                                     int infoLevel = 0) const;

        virtual long long dataSize() const { return _hrs->dataSize(); }

        virtual long long numRecords() const { return _hrs->numRecords(); }


    public:
        //
        // Not in RecordStore interface
        //

        typedef std::map<DiskLoc, boost::shared_array<char> > Records;

        bool isCapped() const { return _hrs->isCapped(); }
        void setCappedDeleteCallback(CappedDocumentDeleteCallback* cb) { _hrs->setCappedDeleteCallback(cb); }
        bool cappedMaxDocs() const { return _hrs->cappedMaxDocs(); }
        bool cappedMaxSize() const { return _hrs->cappedMaxSize(); }

        HeapRecordStore* _hrs;
        TwitterCUD& _tcud;
    };

    class TwitterRecordIterator : public HeapRecordIterator {
    };

    class TwitterRecordReverseIterator : public HeapRecordReverseIterator {
    };

} // namespace mongo
