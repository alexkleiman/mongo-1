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

    HeapRecoveryUnit::HeapRecoveryUnit() { 
    }

    void HeapRecoveryUnit::beginUnitOfWork() {
        for(vector<HrsChange *>::iterator it = _changes.begin(); it != _changes.end(); ++it){
            delete *it;
        }

        _changes.clear();
        invariant(_changes.empty());
    }

    // TODO figure out if this should be sure to do nothing if
    // beginUnitOfWork() hasn't been called
    void HeapRecoveryUnit::commitUnitOfWork() {
        for (vector<HrsChange *>::iterator it = _changes.begin(); it != _changes.end(); ++it){
            delete *it;
        }

        _changes.clear();
        invariant(_changes.empty());
    }

    void HeapRecoveryUnit::endUnitOfWork() {
        for(vector<HrsChange *>::reverse_iterator it = _changes.rbegin(); it != _changes.rend();
                ++it){

            HrsChange* change = *it;
            HeapRecordStore& hrs = change->recordStore();

            // undo the modification
            switch (change->opType()){
                case INSERT:
                    invariant(hrs.hasRecordFor(change->loc()));
                    hrs.simpleDelete(change->loc());
                    invariant(!hrs.hasRecordFor(change->loc()));
                    break;
                case DELETE:
                    invariant(!hrs.hasRecordFor(change->loc()));
                    hrs.simpleInsert(change->loc(), change->record());
                    break;
                case UPDATE:
                    invariant(hrs.hasRecordFor(change->loc()));
                    hrs.simpleDelete(change->loc());
                    invariant(!hrs.hasRecordFor(change->loc()));
                    hrs.simpleInsert(change->loc(), change->record());
                    break;
                default:
                    invariant(!"failed to account for an OpType_t");
            }

            // delete the HrsChange
            delete change;
        }

        _changes.clear();
        invariant(_changes.empty());
    } 

    bool HeapRecoveryUnit::awaitCommit() {
        return false;
    }

    bool HeapRecoveryUnit::commitIfNeeded(bool force) {
        return false;
    }

    bool HeapRecoveryUnit::isCommitNeeded() const {
        return false;
    }

    void* HeapRecoveryUnit::writingPtr(void* data, size_t len) {
        invariant(!"writingPtr should not be called from within HeapRecoveryUnit");
    }

    void HeapRecoveryUnit::syncDataAndTruncateJournal() {
        // no-op
    }

    void HeapRecoveryUnit::declareWriteIntent(const DiskLoc& loc, const OpType_t& ot, 
            HeapRecordStore& hrs) {

        if (ot == INSERT){
            invariant(!hrs.hasRecordFor(loc));

            // there is no old data to save, so we use the HrsChange constructor
            // that does not take in a data argument to indicate this
            _changes.push_back(new HrsChange(loc, INSERT, hrs));
        } else {
            Record* rec = hrs.recordFor(loc);

            // save the old data by pushing it into the changes vector
            _changes.push_back(new HrsChange(loc, *rec, ot, hrs));
        }
    }

} // namespace mongo
