// berkeley1_engine.h

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

#include <db_cxx.h>
#include <string>

#include "mongo/db/storage/storage_engine.h"

namespace mongo {

    class Berkeley1DatabaseCatalogEntry;

    class Berkeley1Engine : public StorageEngine {
    public:

        // TODO figure out why  _environment(0) works. Implicits?
        Berkeley1Engine(): _environment(0) { openEnvironment(_environment, 0); }

        ~Berkeley1Engine() { closeEnvironment(_environment); }

        virtual RecoveryUnit* newRecoveryUnit(OperationContext* opCtx);

        virtual void listDatabases(std::vector<std::string>* out) const;

        virtual DatabaseCatalogEntry* getDatabaseCatalogEntry(OperationContext* opCtx,
                                                              const StringData& db);

        /**
         * @return number of files flushed
         */
        virtual int flushAllFiles(bool sync);

        virtual Status repairDatabase(OperationContext* tnx,
                                      const std::string& dbName,
                                      bool preserveClonedFilesOnFailure = false,
                                      bool backupOriginalFiles = false);

        // not in StorageEngine interface
        DbEnv& environment() { return _environment; }

    private:
        /**
         * extracts the db name from a file ending in .ns
         */
        std::string extractDbName(std::string fileName) const;

        /**
         * Opens a db, handles all exceptions, and returns a bool indicating success
         */
        bool openDB(Db& db, const string& name);

        /**
         * Opens an environment
         */
        void openEnvironment(DbEnv& env, uint32_t extraFlags);

        /**
         * Closes an environment, handles all exceptions, and returns a bool indicating success
         */
        bool closeEnvironment(DbEnv& env);

        DbEnv _environment;
    };
}
