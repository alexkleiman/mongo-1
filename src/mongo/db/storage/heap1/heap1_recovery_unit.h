// heap1_recovery_unit.h

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

#include "mongo/db/storage/recovery_unit.h"

namespace mongo {

    class Heap1RecoveryUnit : public RecoveryUnit {
    public:
        Heap1RecoveryUnit() {
            rollbackPossible = true;
        }

        virtual void beginUnitOfWork() {}
        virtual void commitUnitOfWork() {}

        virtual void endUnitOfWork() {}

        virtual bool commitIfNeeded(bool force = false) {
            return false;
        }

        virtual bool awaitCommit() {
            return true;
        }

        virtual bool isCommitNeeded() const {
            return false;
        }

        virtual void registerChange(Change* change) {
        }

        virtual void* writingPtr(void* data, size_t len) {
            return data;
        }

        virtual void syncDataAndTruncateJournal() {}

        bool rollbackPossible;

    private:
        bool _inUnitOfWork;

        typedef boost::shared_ptr<HeapStoreChange> changePtr;

        // a stack of changes made. All changes will be undone during a rollback
        stack<changePtr> _changes;

        // Private classes that we will use

        /**
         * A simple container interface to describe an operation which modifies a
         * HeapRecordStore. It holds:
         */
        class HeapStoreChange {
            MONGO_DISALLOW_COPYING(HeapStoreChange);
        protected:
            HeapStoreChange(DiskLoc loc, HeapRecordStore* hrs): _loc(loc), _hrs(hrs) { }
            virtual ~HeapStoreChange(){ }

        protected:
            const DiskLoc _loc;

            // Not owned by this class, but rather, just holding a pointer.
            // The pointer must outlive this class.
            HeapRecordStore* _hrs;

        public:
            /**
             * Undo the change that this class represents
             */
            virtual void undo() = 0;
        };

        /**
         * Represents an insertion made to a HeapRecordStore
         */
        class HeapStoreInsert : public HeapStoreChange {
            MONGO_DISALLOW_COPYING(HeapStoreInsert);
        public:
            HeapStoreInsert(const DiskLoc loc, HeapRecordStore* hrs): HeapStoreChange(loc, hrs) { }

            void undo();
        };

        /**
         * Represents a delete made to a HeapRecordStore
         */
        class HeapStoreDelete : public HeapStoreChange {
            MONGO_DISALLOW_COPYING(HeapStoreDelete);
        private:
            boost::shared_array<char> const _rec;

        public:
            HeapStoreDelete(const DiskLoc loc, HeapRecordStore* hrs, boost::shared_array<char> rec):
                HeapStoreChange(loc, hrs), _rec(rec) { }

            void undo();
        };

        /**
         * Represents an update made to a HeapRecordStore
         */
        class HeapStoreUpdate : public HeapStoreChange {
            MONGO_DISALLOW_COPYING(HeapStoreUpdate);
        private:
            boost::shared_array<char> _rec;

        public:
            HeapStoreUpdate(const DiskLoc loc, HeapRecordStore* hrs,
                    const boost::shared_array<char> data);

            void undo();
        };
    };

}
