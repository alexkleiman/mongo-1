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

#include "mongo/db/operation_context_berkeley.h"

#include <db_cxx.h>

namespace mongo {
    OperationContextBerkeley::OperationContextBerkeley(DbEnv& env) : 
            _environment(env){
        _recoveryUnit.reset(new Berkeley1RecoveryUnit(_environment));
    }

    OperationContextBerkeley::~OperationContextBerkeley() {
    }

    CurOp* OperationContextBerkeley::getCurOp() const {
        invariant(false);
        return NULL;
    }
    RecoveryUnit* OperationContextBerkeley::recoveryUnit() const {
        return _recoveryUnit.get();
    }

    LockState* OperationContextBerkeley::lockState() const {
        // TODO: Eventually, this should return an actual LockState object. For now,
        //       LockState depends on the whole world and is not necessary for testing.
        return NULL;
    }

    ProgressMeter* OperationContextBerkeley::setMessage(const char * msg,
                                      const std::string &name,
                                      unsigned long long progressMeterTotal,
                                      int secondsBetween) {
        invariant(false);
        return NULL;
    }

    void OperationContextBerkeley::checkForInterrupt(bool heedMutex) const {

    }

    Status OperationContextBerkeley::checkForInterruptNoAssert() const {
        return Status::OK();
    }

    bool OperationContextBerkeley::isPrimaryFor( const StringData& ns ) {
        return true;
    }

    const char * OperationContextBerkeley::getNS() const {
        return NULL;
    }
}  // namespace mongo
