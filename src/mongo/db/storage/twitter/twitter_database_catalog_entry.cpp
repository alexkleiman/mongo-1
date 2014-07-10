// twitter_database_catalog_entry.cpp

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

#include "mongo/db/storage/twitter/twitter_database_catalog_entry.h"

#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/index/2d_access_method.h"
#include "mongo/db/index/btree_access_method.h"
#include "mongo/db/index/fts_access_method.h"
#include "mongo/db/index/hash_access_method.h"
#include "mongo/db/index/haystack_access_method.h"
#include "mongo/db/index/index_access_method.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/index/s2_access_method.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/storage/twitter/twitter_btree_impl.h"
#include "mongo/db/storage/twitter/twitter_recovery_unit.h"
#include "mongo/db/storage/twitter/twitter_record_store.h"

namespace mongo {

    TwitterDatabaseCatalogEntry::TwitterDatabaseCatalogEntry( const StringData& name,
            Heap1DatabaseCatalogEntry* hdce): DatabaseCatalogEntry( name ), _hdce(hdce) { }

    TwitterDatabaseCatalogEntry::~TwitterDatabaseCatalogEntry() {
    }

    bool TwitterDatabaseCatalogEntry::isEmpty() const {
        return _hdce->isEmpty();
    }

    bool TwitterDatabaseCatalogEntry::exists() const {
        return _hdce->exists();
    }

    void TwitterDatabaseCatalogEntry::appendExtraStats( OperationContext* opCtx,
                                                      BSONObjBuilder* out,
                                                      double scale ) const {
        _hdce->appendExtraStats(opCtx, out, scale);
    }

    CollectionCatalogEntry* TwitterDatabaseCatalogEntry::getCollectionCatalogEntry( OperationContext* opCtx,
                                                                                  const StringData& ns ) const {
        return _hdce->getCollectionCatalogEntry(opCtx, ns);
    }

    RecordStore* TwitterDatabaseCatalogEntry::getRecordStore( OperationContext* opCtx,
                                                            const StringData& ns ) {
        invariant(!"nyi");
    }

    void TwitterDatabaseCatalogEntry::getCollectionNamespaces( std::list<std::string>* out ) const {
        invariant(!"nyi");
    }

    Status TwitterDatabaseCatalogEntry::createCollection( OperationContext* opCtx,
                                                        const StringData& ns,
                                                        const CollectionOptions& options,
                                                        bool allocateDefaultSpace ) {
        invariant(!"nyi");
    }

    Status TwitterDatabaseCatalogEntry::dropCollection( OperationContext* opCtx,
                                                      const StringData& ns ) {
        invariant(!"nyi");
    }


    IndexAccessMethod* TwitterDatabaseCatalogEntry::getIndex( OperationContext* txn,
                                                            const CollectionCatalogEntry* collection,
                                                            IndexCatalogEntry* index ) {
        invariant(!"nyi");
    }

    Status TwitterDatabaseCatalogEntry::renameCollection( OperationContext* txn,
                                                        const StringData& fromNS,
                                                        const StringData& toNS,
                                                        bool stayTemp ) {
        invariant(!"nyi");
    }
}
