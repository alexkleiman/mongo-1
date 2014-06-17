// berkeley_recovery_unit.cpp

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

#include "mongo/db/storage/berkeley/berkeley_recovery_unit.h"

#include <db_cxx.h>

#include "mongo/db/storage/record.h"

namespace mongo {

    void BerkeleyRecoveryUnit::beginUnitOfWork() {
        invariant(_bdbTransaction == NULL);
        _bdbEnv.txn_begin(NULL, &_bdbTransaction, _transactionFlags);
    }

    void BerkeleyRecoveryUnit::commitUnitOfWork() {
        invariant(_bdbTransaction != NULL);
        _bdbTransaction->commit(_commitFlags);

        // TODO figure out what to do in cases of error
        
        // start a new transaction so that further calls to commitUnitOfWork() succeed
        _bdbEnv.txn_begin(NULL, &_bdbTransaction, _transactionFlags);
    }

    void BerkeleyRecoveryUnit::endUnitOfWork() {
        invariant(_bdbTransaction != NULL);
        _bdbTransaction->abort();

        // TODO figure out what to do in cases of error
    }

    bool BerkeleyRecoveryUnit::awaitCommit() {
        invariant(!"awaitCommit should never be called on a BerkeleyRecoveryUnit");
    }

    bool BerkeleyRecoveryUnit::commitIfNeeded(bool force) {
        invariant(!"commitIfNeeded should never be called on a BerkeleyRecoveryUnit");
    }

    bool BerkeleyRecoveryUnit::isCommitNeeded() const {
        invariant(!"isCommitNeeded should never be called on a BerkeleyRecoveryUnit");
    }

    void* BerkeleyRecoveryUnit::writingPtr(void* data, size_t len) {
        invariant(!"writingPtr should never be called on a BerkeleyRecoveryUnit");
    }

    void BerkeleyRecoveryUnit::syncDataAndTruncateJournal() {
        invariant(_bdbTransaction != NULL);
        _bdbEnv.txn_checkpoint(0, 0, 0);

        // TODO figure out what to do in case of error
    }

    DbTxn* BerkeleyRecoveryUnit::getCurrentTransaction() {
        return _bdbTransaction;
    }

} // namespace mongo
