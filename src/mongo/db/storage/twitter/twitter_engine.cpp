// twitter_engine.cpp

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

#include "mongo/db/storage/twitter/twitter_engine.h"

#include <fstream>

#include "mongo/db/operation_context_noop.h"
#include "mongo/db/storage/heap1/heap1_database_catalog_entry.h"
#include "mongo/db/storage/heap1/heap1_recovery_unit.h"
#include "mongo/db/storage/twitter/twitter_database_catalog_entry.h"
#include "mongo/util/log.h"

namespace mongo {

    bool isColon(string s) {
        return s.compare(":") == 0;
    }

    TwitterEngine::TwitterEngine(): _heapEngine() {
        std::ifstream file("loaded.mongo");
        std::string str; 

        vector<string> lines;

        while (std::getline(file, str)) {
            string newString = str;
            lines.insert(lines.begin(), newString);
        }

        set<string> namespaces;

        for (vector<string>::iterator it = lines.begin(); it != lines.end(); ++it) {
            vector<string> splitVec;
            string toSplit(*it);

            //getline(toSplit, 

            invariant(splitVec.size() == 3);

            OperationContextNoop txn;
            DatabaseCatalogEntry* dbce = _heapEngine.getDatabaseCatalogEntry(&txn, "db");

            string collectionNamespace = splitVec[0];
            string diskLocString = splitVec[1];
            string bsonString = splitVec[2];

            if (namespaces.find(collectionNamespace) != namespaces.end()){
                namespaces.insert(collectionNamespace);
                dbce->createCollection(&txn, StringData(collectionNamespace), CollectionOptions(), true);
            }

            if (bsonString.compare("Delete") == 0){

            } else {

            }
        }
    }

    RecoveryUnit* TwitterEngine::newRecoveryUnit( OperationContext* opCtx ) {
        return new Heap1RecoveryUnit();
    }

    void TwitterEngine::listDatabases( std::vector<std::string>* out ) const {
        _heapEngine.listDatabases(out);
    }

    DatabaseCatalogEntry* TwitterEngine::getDatabaseCatalogEntry( OperationContext* opCtx,
                                                                const StringData& dbName ) {
        return new TwitterDatabaseCatalogEntry(dbName, new Heap1DatabaseCatalogEntry(dbName));
    }

    int TwitterEngine::flushAllFiles(bool sync) {
        return _heapEngine.flushAllFiles(sync);
    }

    Status TwitterEngine::repairDatabase( OperationContext* tnx,
                                          const std::string& dbName,
                                          bool preserveClonedFilesOnFailure,
                                          bool backupOriginalFiles) {

        return _heapEngine.repairDatabase(
                tnx, dbName, preserveClonedFilesOnFailure, backupOriginalFiles);
    }
}
