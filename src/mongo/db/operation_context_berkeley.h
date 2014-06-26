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

#include <boost/scoped_ptr.hpp>
#include <db_cxx.h>
#include <string>

#include "mongo/db/operation_context.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/db/storage/berkeley1/berkeley1_recovery_unit.h"
#pragma once

namespace mongo {

    class OperationContextBerkeley : public OperationContext {
    public:
        OperationContextBerkeley(DbEnv& env);

        virtual ~OperationContextBerkeley();

        CurOp* getCurOp() const;
        virtual RecoveryUnit* recoveryUnit() const;

        virtual LockState* lockState() const;

        virtual ProgressMeter* setMessage(const char * msg,
                                          const std::string &name,
                                          unsigned long long progressMeterTotal,
                                          int secondsBetween);

        virtual void checkForInterrupt(bool heedMutex = true) const;

        virtual Status checkForInterruptNoAssert() const;

        virtual bool isPrimaryFor( const StringData& ns );

        virtual const char * getNS() const;

        virtual Transaction* getTransaction();

        // not in OperationContext API
        DbEnv& getEnv() { return _environment; }

    private:
        boost::scoped_ptr<Berkeley1RecoveryUnit> _recoveryUnit;
        DbEnv& _environment;
    };

}  // namespace mongo
