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
#include <boost/filesystem/operations.hpp>
#include <db_cxx.h>

#include "mongo/db/operation_context.h"
#include "mongo/db/storage_options.h"
#include "mongo/db/storage/berkeley1/berkeley1_catalog_entry.h"
#include "mongo/db/storage/berkeley1/berkeley1_recovery_unit.h"
//#include "mongo/db/storage_options.h"

namespace mongo {

    // TODO figure out why  _environment(0) works. Implicits?
    Berkeley1Engine::Berkeley1Engine(): _environment(0), _path("berkeley") {
        uint32_t cFlags_ = (DB_CREATE     | // If the environment does not
                                            // exist, create it.
                            DB_INIT_LOCK  | // Initialize locking
                            DB_INIT_LOG   | // Initialize logging
                            DB_INIT_MPOOL | // Initialize the cache
                            DB_THREAD     | // Free-thread the env handle.
                            DB_INIT_TXN);

        // TODO, integrate storageGlobalParams.dbpath
        boost::filesystem::path dir(_path);

        // Not sure if needed
        boost::filesystem::create_directory(dir);
        _environment.open(_path.data(), cFlags_, 0);
    }

    RecoveryUnit* Berkeley1Engine::newRecoveryUnit(OperationContext* opCtx) {
        return new Berkeley1RecoveryUnit(_environment);
    }

    string Berkeley1Engine::extractDbName(string fileName) const {
        string::size_type nameLength = fileName.length();
        invariant(nameLength > 3);

        return fileName.substr(0, nameLength - 3);
    }

    void Berkeley1Engine::listDatabases(std::vector<std::string>* out) const {
        boost::filesystem::path path("/berkeleyEnv");
        for (boost::filesystem::directory_iterator it(path); 
                it != boost::filesystem::directory_iterator();
              ++it) {
            if (storageGlobalParams.directoryperdb) {
                boost::filesystem::path p = *it;
                string fileName = p.filename().string();
                if (exists(p)) {
                    out->push_back(extractDbName(fileName));
                }
            }
            else {
                string fileName = boost::filesystem::path(*it).filename().string();
                if (fileName.length() > 3 && fileName.substr(fileName.length() - 3, 3) == ".ns")
                    out->push_back(fileName.substr(0, fileName.length() - 3));
            }
        }
    }

    DatabaseCatalogEntry* Berkeley1Engine::getDatabaseCatalogEntry(OperationContext* opCtx,
            const StringData& db) {
        return NULL;

        //return new Berkeley1DatabaseCatalogEntry(db,
                                                //storageGlobalParams.dbpath,
                                                //_environment,
                                                //storageGlobalParams.directoryperdb);
    }

    bool Berkeley1Engine::openDB(Db& db, const string& name) {

        static uint32_t cFlags_ = (DB_CREATE | DB_AUTO_COMMIT | DB_READ_UNCOMMITTED);

        try {
            db.open(NULL, name.data(), NULL, DB_BTREE, cFlags_, 0);
        } catch(DbException &e) {
            error() << "Error opening database: " << name << "\n";
            error() << e.what() << std::endl;
            return false;
        } catch(std::exception &e) {
            error() << "Error opening database: " << name << "\n";
            error() << e.what() << std::endl;
            return false;
        }

        return true;
    }

    int Berkeley1Engine::flushAllFiles(bool sync) {
        vector<std::string> databases;

        // get the names of all the databases
        listDatabases(&databases);

        int numFlushed = 0;

        for (vector<string>::iterator it; it != databases.end(); ++it) {
            string fullPath = storageGlobalParams.dbpath + *it + ".db";
            Db db(&_environment, 0);

            // open the database, worry about exceptions in the helper method
            if (!openDB(db, *it)){
                continue;
            }

            db.sync(0);
            ++numFlushed;
            db.close(0);
        }

        return numFlushed;
    }

    Status Berkeley1Engine::repairDatabase(OperationContext* tnx,
            const std::string& dbName,
            bool preserveClonedFilesOnFailure,
            bool backupOriginalFiles) {
        invariant(!"not yet implemented");
    }

} // namespace mongo
