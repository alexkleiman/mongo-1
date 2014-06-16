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

#include "mongo/db/operation_context_impl.h"
// Probably include Berkley header here


namespace mongo {

    class OperationContextBerkeley : public OperationContext {
    public:
        OperationContextBerkeley(/* DbEnv env */) _environment(OpenEnv()){
            // Open the Environment, DB

            _recoveryUnit.reset(new BerkeleyRecoveryUnit(_environment));
        }

        virtual ~OperationContextBerkeley() {

        }

        CurOp* getCurOp() const {
            invariant(false);
            return NULL;
        }
        virtual RecoveryUnit* recoveryUnit() const {
            return _recoveryUnit.get();
        }

        virtual LockState* lockState() const {
            // TODO: Eventually, this should return an actual LockState object. For now,
            //       LockState depends on the whole world and is not necessary for testing.
            return NULL;
        }

        virtual ProgressMeter* setMessage(const char * msg,
                                          const std::string &name,
                                          unsigned long long progressMeterTotal,
                                          int secondsBetween) {
            invariant(false);
            return NULL;
        }

        virtual void checkForInterrupt(bool heedMutex = true) const { }

        virtual Status checkForInterruptNoAssert() {
            return Status::OK();
        }

        virtual bool isPrimaryFor( const StringData& ns ) {
            return true;
        }

        virtual const char * getNS() const {
            return NULL;
        }

        virtual Env OpenEnv() {
            DbEnv env("berkeleyEnv/");
            uint32_t cFlags_ = (DB_CREATE     | // If the environment does not
                                                // exist, create it.
                                DB_INIT_LOCK  | // Initialize locking
                                DB_INIT_LOG   | // Initialize logging
                                DB_INIT_MPOOL | // Initialize the cache
                                DB_THREAD     | // Free-thread the env handle.
                                DB_INIT_TXN);
            env.open(envHome_.c_str(), cFlags_, 0);

            return env;
        }
    };

}  // namespace mongo
