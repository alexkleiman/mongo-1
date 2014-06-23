// berkeley1_engine.cpp

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

#include "db/storage/berkeley1/berkeley1_engine.h"

#include <boost/filesystem.hpp>
#include <db_cxx.h>

#include "mongo/db/operation_context.h"
#include "mongo/db/storage/berkeley1/berkeley1_recovery_unit.h"

namespace mongo {

    // TODO figure out why  _environment(0) works. Implicits?
    Berkeley1Engine::Berkeley1Engine(): _environment(0) {
        uint32_t cFlags_ = (DB_CREATE     | // If the environment does not
                                            // exist, create it.
                            DB_INIT_LOCK  | // Initialize locking
                            DB_INIT_LOG   | // Initialize logging
                            DB_INIT_MPOOL | // Initialize the cache
                            DB_THREAD     | // Free-thread the env handle.
                            DB_INIT_TXN);

        boost::filesystem::path dir("berkeleyEnv");
        boost::filesystem::create_directory(dir);
        _environment.open("berkeleyEnv", cFlags_, 0);
    }

    RecoveryUnit* Berkeley1Engine::newRecoveryUnit(OperationContext* opCtx) {
        return new Berkeley1RecoveryUnit(_environment);
    }

    void Berkeley1Engine::listDatabases(std::vector<std::string>* out) const {
        invariant(!"not yet implemented");
    }

    DatabaseCatalogEntry* Berkeley1Engine::getDatabaseCatalogEntry(OperationContext* opCtx,
            const StringData& db) {
        invariant(!"not yet implemented");
    }

    int Berkeley1Engine::flushAllFiles(bool sync) {
        invariant(!"not yet implemented");
    }

    Status Berkeley1Engine::repairDatabase(OperationContext* tnx,
            const std::string& dbName,
            bool preserveClonedFilesOnFailure,
            bool backupOriginalFiles) {
        invariant(!"not yet implemented");
    }

} // namespace mongo
