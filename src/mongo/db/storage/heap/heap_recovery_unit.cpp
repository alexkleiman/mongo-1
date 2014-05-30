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

#include "mongo/db/storage/heap/heap_recovery_unit.h"

#include "mongo/db/storage/record.h"

namespace mongo {

    HeapRecoveryUnit::HeapRecoveryUnit(): _inUnitOfWork(false) { }

    void HeapRecoveryUnit::beginUnitOfWork() {
        invariant(_changes.empty());
        _inUnitOfWork = true;
    }

    void HeapRecoveryUnit::commitUnitOfWork() {
        invariant(_inUnitOfWork);

        // sadly, the stack class does not contain a clear() method
        while (!_changes.empty()){
            _changes.pop();
        }
    }

    void HeapRecoveryUnit::endUnitOfWork() {
        while (!_changes.empty()) {
            HeapStoreChange* change = _changes.top().get();
            change->undo();
            _changes.pop();
        }

        invariant(_changes.empty());
        _inUnitOfWork = false;
    }

    bool HeapRecoveryUnit::awaitCommit() {
        return true;
    }

    bool HeapRecoveryUnit::commitIfNeeded(bool force) {
        return false;
    }

    bool HeapRecoveryUnit::isCommitNeeded() const {
        return false;
    }

    void* HeapRecoveryUnit::writingPtr(void* data, size_t len) {
        return data;
    }

    void HeapRecoveryUnit::syncDataAndTruncateJournal() {
        // no-op
    }

    void HeapRecoveryUnit::declareWriteIntent(const DiskLoc& loc, const OpType ot,
            HeapRecordStore* hrs) {

        // TODO remove this line and actually call the function once the rest of the code
        // uses WriteUnitOfWork when modifying a record store
        return;

        invariant(_inUnitOfWork);

        if (ot == INSERT){
            invariant(!hrs->hasRecordFor(loc));
            _changes.push(changePtr(new HeapStoreInsert(loc, hrs)));
        } else if (ot == UPDATE) {
            boost::shared_array<char> data = hrs->simpleLookup(loc);

            // save the old data by pushing it into the changes vector
            _changes.push(changePtr(new HeapStoreUpdate(loc, hrs, data)));
        } else {
            invariant(ot == DELETE);

            boost::shared_array<char> data = hrs->simpleLookup(loc);

            // save the old data by pushing it into the changes vector
            _changes.push(changePtr(new HeapStoreDelete(loc, hrs, data)));
        }
    }

    // HeapStore* implementations

    // HeapStoreInsert
    void HeapRecoveryUnit::HeapStoreInsert::undo() {
        invariant(_hrs->hasRecordFor(_loc));
        _hrs->simpleDelete(_loc);
        invariant(!_hrs->hasRecordFor(_loc));
    }

    // HeapStoreDelete
    void HeapRecoveryUnit::HeapStoreDelete::undo() {
        invariant(!_hrs->hasRecordFor(_loc));
        _hrs->simpleInsert(_loc, _rec);
    }

    // HeapStoreUpdate
    HeapRecoveryUnit::HeapStoreUpdate::HeapStoreUpdate(const DiskLoc loc, HeapRecordStore* hrs,
                                                      boost::shared_array<char> data):
                                                          HeapStoreChange(loc, hrs) {

        Record* record = reinterpret_cast<Record*>(data.get());
        _rec = boost::shared_array<char>(new char[record->lengthWithHeaders()]);
        memcpy(_rec.get(), data.get(), record->lengthWithHeaders());
    }

    void HeapRecoveryUnit::HeapStoreUpdate::undo() {
        invariant(_hrs->hasRecordFor(_loc));

        _hrs->simpleUpdate(_loc, _rec);
    }

} // namespace mongo
