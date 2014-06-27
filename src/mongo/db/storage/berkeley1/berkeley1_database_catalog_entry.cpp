// berkeley1_database_catalog_entry.cpp

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

#include "mongo/db/storage/berkeley1/berkeley1_database_catalog_entry.h"

#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/index/btree_access_method.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/storage/berkeley1/berkeley1_btree_impl.h"
#include "mongo/db/storage/berkeley1/berkeley1_recovery_unit.h"
#include "mongo/db/structure/record_store_berkeley.h"

#include <boost/filesystem.hpp>
#include <boost/filesystem/operations.hpp>

namespace mongo {

    Berkeley1DatabaseCatalogEntry::Berkeley1DatabaseCatalogEntry( const StringData& name,
                                                                  const StringData& path_name,
                                                                  DbEnv& env,
                                                                  bool directoryperdb)
        : DatabaseCatalogEntry( name ),
          _path( path_name.toString() ),
          _env( env ),
          _directoryperdb(directoryperdb) {
        _everHadACollection = false;

        boost::filesystem::path path(path_name.toString());
        for (boost::filesystem::directory_iterator it(path);
                it != boost::filesystem::directory_iterator();
              ++it) {
            if (directoryperdb) {
                //TODO
            }
            else {
                string fileName = boost::filesystem::path(*it).filename().string();

                if (fileName.size() < name.toString().size() + 5)
                    continue;                

                if ((name.toString() + ".").compare(0, name.toString().size() + 1, fileName))
                    continue;

                if (fileName.substr(fileName.length() - 3, 3) != ".db")
                    continue;

                string ns_string = fileName.substr(0, fileName.length() - 3);
                StringData ns(ns_string);
                
                // Create entry, record store for collection
                Entry*& entry = _entryMap[ ns.toString() ];
                invariant ( !entry );

                entry = new Entry( ns );

                // TODO figure out if capped
                entry->rs.reset( new BerkeleyRecordStore( env, ns ) );
            }
        }
    }

    Berkeley1DatabaseCatalogEntry::~Berkeley1DatabaseCatalogEntry() {
        for ( EntryMap::const_iterator i = _entryMap.begin(); i != _entryMap.end(); ++i ) {
            delete i->second;
        }
        _entryMap.clear();
    }

    bool Berkeley1DatabaseCatalogEntry::isEmpty() const {
        boost::mutex::scoped_lock lk( _entryMapLock );
        return _entryMap.empty();
    }

    void Berkeley1DatabaseCatalogEntry::appendExtraStats( OperationContext* opCtx,
                                                      BSONObjBuilder* out,
                                                      double scale ) const {
    }

    CollectionCatalogEntry* Berkeley1DatabaseCatalogEntry::getCollectionCatalogEntry( OperationContext* opCtx,
                                                                                  const StringData& ns ) {
        boost::mutex::scoped_lock lk( _entryMapLock );
        EntryMap::iterator i = _entryMap.find( ns.toString() );
        if ( i == _entryMap.end() )
            return NULL;
        return i->second;
    }

    RecordStore* Berkeley1DatabaseCatalogEntry::getRecordStore( OperationContext* opCtx,
                                                            const StringData& ns ) {
        boost::mutex::scoped_lock lk( _entryMapLock );
        EntryMap::iterator i = _entryMap.find( ns.toString() );
        if ( i == _entryMap.end() )
            return NULL;
        return i->second->rs.get();
    }

    void Berkeley1DatabaseCatalogEntry::getCollectionNamespaces( std::list<std::string>* out ) const {
        boost::mutex::scoped_lock lk( _entryMapLock );
        for ( EntryMap::const_iterator i = _entryMap.begin(); i != _entryMap.end(); ++i ) {
            out->push_back( i->first );
        }
    }

    Status Berkeley1DatabaseCatalogEntry::createCollection( OperationContext* opCtx,
                                                        const StringData& ns,
                                                        const CollectionOptions& options,
                                                        bool allocateDefaultSpace ) {

        // dynamic_cast<Heap1RecoveryUnit*>( opCtx->recoveryUnit() )->rollbackPossible = false;
        boost::mutex::scoped_lock lk( _entryMapLock );
        Entry*& entry = _entryMap[ ns.toString() ];
        if ( entry )
            return Status( ErrorCodes::NamespaceExists,
                           "cannot create collection, already exists" );

        entry = new Entry( ns );

        if ( options.capped ) {
            entry->rs.reset(new BerkeleyRecordStore(_env,
                                                ns,
                                                true,
                                                options.cappedSize
                                                     ? options.cappedSize : 4096, // default size
                                                options.cappedMaxDocs
                                                     ? options.cappedMaxDocs : -1)); // no limit
        }
        else {
            entry->rs.reset( new BerkeleyRecordStore( _env, ns ) );
        }

        return Status::OK();
    }

    Status Berkeley1DatabaseCatalogEntry::dropCollection( OperationContext* opCtx,
                                                      const StringData& ns ) {
        //TODO: invariant( opCtx->lockState()->isWriteLocked( ns ) );

        // dynamic_cast<Heap1RecoveryUnit*>( opCtx->recoveryUnit() )->rollbackPossible = false;
        boost::mutex::scoped_lock lk( _entryMapLock );
        EntryMap::iterator i = _entryMap.find( ns.toString() );

        if ( i == _entryMap.end() )
            return Status( ErrorCodes::NamespaceNotFound, "namespace not found" );

        delete i->second;
        _entryMap.erase( i );

        //TODO does this need to delete disk files??

        return Status::OK();
    }


    IndexAccessMethod* Berkeley1DatabaseCatalogEntry::getIndex( OperationContext* txn,
                                                            const CollectionCatalogEntry* collection,
                                                            IndexCatalogEntry* index ) {
        const IndexDescriptor* desc = index->descriptor();

        std::auto_ptr<Berkeley1BtreeImpl> raw( new Berkeley1BtreeImpl( _env, collection->ns().ns(),
                                                                    desc->indexName() ) );

        return new BtreeAccessMethod( index, raw.release() );
    }

    Status Berkeley1DatabaseCatalogEntry::renameCollection( OperationContext* txn,
                                                        const StringData& fromNS,
                                                        const StringData& toNS,
                                                        bool stayTemp ) {
        invariant(!"nyi");
    }

    // ------------------

    Berkeley1DatabaseCatalogEntry::Entry::Entry( const StringData& ns)
        : CollectionCatalogEntry( ns ) {
    }

    Berkeley1DatabaseCatalogEntry::Entry::~Entry() {
        for ( Indexes::const_iterator i = indexes.begin(); i != indexes.end(); ++i )
            delete i->second;
        indexes.clear();
    }

    int Berkeley1DatabaseCatalogEntry::Entry::getTotalIndexCount() const {
        return static_cast<int>( indexes.size() );
    }

    int Berkeley1DatabaseCatalogEntry::Entry::getCompletedIndexCount() const {
        int ready = 0;
        for ( Indexes::const_iterator i = indexes.begin(); i != indexes.end(); ++i )
            if ( i->second->ready )
                ready++;
        return ready;
    }

    void Berkeley1DatabaseCatalogEntry::Entry::getAllIndexes( std::vector<std::string>* names ) const {
        for ( Indexes::const_iterator i = indexes.begin(); i != indexes.end(); ++i )
            names->push_back( i->second->name );
    }

    BSONObj Berkeley1DatabaseCatalogEntry::Entry::getIndexSpec( const StringData& idxName ) const {
        Indexes::const_iterator i = indexes.find( idxName.toString() );
        invariant( i != indexes.end() );
        return i->second->spec; 
    }

    bool Berkeley1DatabaseCatalogEntry::Entry::isIndexMultikey( const StringData& idxName) const {
        Indexes::const_iterator i = indexes.find( idxName.toString() );
        invariant( i != indexes.end() );
        return i->second->isMultikey;
    }

    bool Berkeley1DatabaseCatalogEntry::Entry::setIndexIsMultikey(OperationContext* txn,
                                                              const StringData& idxName,
                                                              bool multikey ) {
        Indexes::const_iterator i = indexes.find( idxName.toString() );
        invariant( i != indexes.end() );
        if (i->second->isMultikey == multikey)
            return false;

        i->second->isMultikey = multikey;
        return true;
    }

    DiskLoc Berkeley1DatabaseCatalogEntry::Entry::getIndexHead( const StringData& idxName ) const {
        Indexes::const_iterator i = indexes.find( idxName.toString() );
        invariant( i != indexes.end() );
        return i->second->head;
    }

    void Berkeley1DatabaseCatalogEntry::Entry::setIndexHead( OperationContext* txn,
                                                         const StringData& idxName,
                                                         const DiskLoc& newHead ) {
        Indexes::const_iterator i = indexes.find( idxName.toString() );
        invariant( i != indexes.end() );
        i->second->head = newHead;
    }

    bool Berkeley1DatabaseCatalogEntry::Entry::isIndexReady( const StringData& idxName ) const {
        Indexes::const_iterator i = indexes.find( idxName.toString() );
        invariant( i != indexes.end() );
        return i->second->ready;
    }

    Status Berkeley1DatabaseCatalogEntry::Entry::removeIndex( OperationContext* txn,
                                                          const StringData& idxName ) {
        //TODO persist this on disk
        indexes.erase( idxName.toString() );
        return Status::OK();
    }

    Status Berkeley1DatabaseCatalogEntry::Entry::prepareForIndexBuild( OperationContext* txn,
                                                                   const IndexDescriptor* spec ) {
        auto_ptr<IndexEntry> newEntry( new IndexEntry() );
        newEntry->name = spec->indexName();
        newEntry->spec = spec->infoObj();
        newEntry->ready = false;
        newEntry->isMultikey = false;

        indexes[spec->indexName()] = newEntry.release();
        return Status::OK();
    }

    void Berkeley1DatabaseCatalogEntry::Entry::indexBuildSuccess( OperationContext* txn,
                                                              const StringData& idxName ) {
        Indexes::const_iterator i = indexes.find( idxName.toString() );
        invariant( i != indexes.end() );
        i->second->ready = true;
    }

    void Berkeley1DatabaseCatalogEntry::Entry::updateTTLSetting( OperationContext* txn,
                                                             const StringData& idxName,
                                                             long long newExpireSeconds ) {
        invariant( false );
    }
}