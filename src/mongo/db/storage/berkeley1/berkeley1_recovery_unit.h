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

#include <string>
#include <stack>

#include "mongo/db/diskloc.h"
#include "mongo/db/storage/berkeley1/mongo_bdb.h"
#include "mongo/db/storage/record.h"
#include "mongo/db/storage/recovery_unit.h"
#include "mongo/platform/compiler.h"

namespace mongo {

    /**
     * A BerkeleyRecoveryUnit ensures that data in a BerkeleyRecordStore persists. All information
     *  stored in the BerkeleyRecordStore must be mutated through this interface
     */
    class Berkeley1RecoveryUnit : public RecoveryUnit {
        MONGO_DISALLOW_COPYING(Berkeley1RecoveryUnit);
    public:

        Berkeley1RecoveryUnit(DbEnv& bdbEnv): _bdbEnv(bdbEnv), _bdbTransaction(NULL) { }

        virtual ~Berkeley1RecoveryUnit() { }

        /**
         * Begins a unit of work, enabling any changes to the record
         * store made through this API to be rolled back if necessary.
         * All non-rolled back and non-committed writes will be committed
         * when this is called
         **/
        void beginUnitOfWork();

        /**
         * Commits all "proper" changes made since the last call to beginUnitOfWork(),
         * so long as beginUnitOfWork() has been called more recently than endUnitOfWork().
         * A "proper" change is one that has been made via a call to
         * BerkeleyRecordStore::insertRecord(), BerkeleyRecordStore::updateRecord or
         * BerkeleyRecordStore::deleteRecord().
         * */
        void commitUnitOfWork();

        /**
         * Rolls back all "proper" changes made since the last call to commitUnitOfWork()
         * A "proper" change is one that has been made via a call to
         * BerkeleyRecordStore::insertRecord(), BerkeleyRecordStore::updateRecord or
         * BerkeleyRecordStore::deleteRecord().
         */
        void endUnitOfWork();

        /**
         * Wait for acknowledgement of the next group commit.
         */
        virtual bool awaitCommit();

        /**
         * Commits, if needed.
         * @return true if a commit was executed, and false otherwise
         */
        virtual bool commitIfNeeded(bool force = false);

        /**
         * Returns whether a commit is needed, but does not commit
         * @return true if a commit is needed, and false otherwise
         */
        virtual bool isCommitNeeded() const;

        // this is here to satisfy the RecoveryUnit interface, but should never be called
        virtual void* writingPtr(void* data, size_t len);

        /**
         * Not applicable to the BerkeleyRecoveryUnit class. Currently a no-op
         */
        virtual void syncDataAndTruncateJournal();

        virtual DbTxn* getCurrentTransaction();

    private:
        DbEnv& _bdbEnv;
        DbTxn* _bdbTransaction;
        static const int _transactionFlags = 0;
        static const int _commitFlags = 0;
    };

} // namespace mongo
