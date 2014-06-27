// Berkeley11_btree_impl.cpp

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

#include "mongo/db/storage/Berkeley1/Berkeley1_btree_impl.h"

#include <string>

#include <db_cxx.h>

#include "mongo/db/storage/Berkeley1/Berkeley1_engine.h"
#include "mongo/db/storage/Berkeley1/Berkeley1_recovery_unit.h"

namespace mongo {

    namespace {
        class Berkeley1Cursor : public BtreeInterface::Cursor {
        public:
            Berkeley1Cursor( Db& db, bool direction )
                : _cursor( 0 ), _direction( direction ), _cached( false ), _isValid( false ) {
                Dbc* cursorrp = _cursor;
                db.cursor(NULL, &cursorrp, 0);
            }

            virtual ~Berkeley1Cursor() {
                _cursor->close();
            }

            int getDirection() const { return _direction; }

            bool isEOF() const {
                return _isValid;
            }

            /**
             * Will only be called with other from same index as this.
             * All EOF locs should be considered equal.
             */
            bool pointsToSamePlaceAs(const Cursor& other) const {
                const Berkeley1Cursor* realOther = dynamic_cast<const Berkeley1Cursor*>( &other );
                if (isEOF() || realOther->isEOF())
                    return false;
                int result;
                invariant(_cursor->cmp(realOther->_cursor, &result, 0) == 0);

                return (result == 0);
            }

            void aboutToDeleteBucket(const DiskLoc& bucket) {
                // don't know if I need this or not
            }

            /** I am asuming this seeks the cursor to the smallest pair which has key greater than
             * or equal to the passed in key, and data >= the passed in loc.
             * Also that it returns true iff the key it seeked to equals the one passed in
             * also, if this fails, and _isValid used to be true, what should happen?
             */
            bool locate(const BSONObj& key, const DiskLoc& loc) {
                _cached = false;
                Dbt dbt_key(const_cast<char*>(key.objdata()), key.objsize());
                Dbt value;

                // TODO make sure this can actually return DB_NOTFOUND
                if ( _cursor->get(&dbt_key, &value, DB_SET_RANGE) == DB_NOTFOUND )
                    return false;


                Dbt dbt_loc(reinterpret_cast<char*>(const_cast<DiskLoc*>(&loc)), sizeof(DiskLoc));
                if ( _cursor->get(&dbt_key, &dbt_loc, DB_GET_BOTH_RANGE) == DB_NOTFOUND ) {
                    if ( _cursor->get(&dbt_key, &value, DB_NEXT_NODUP) == DB_NOTFOUND )
                    return false;
                }

                _load();
                _isValid = true;
                return key.woCompare( _cachedKey, BSONObj(), false ) == 0;
            }

            void advanceTo(const BSONObj &keyBegin,
                           int keyBeginLen,
                           bool afterKey,
                           const vector<const BSONElement*>& keyEnd,
                           const vector<bool>& keyEndInclusive) {
                invariant( !"Berkeley1db has no advanceTo" );
            }

            /**
             * Locate a key with fields comprised of a combination of keyBegin fields and keyEnd
             * fields.
             */
            void customLocate(const BSONObj& keyBegin,
                              int keyBeginLen,
                              bool afterVersion,
                              const vector<const BSONElement*>& keyEnd,
                              const vector<bool>& keyEndInclusive) {
                invariant( !"Berkeley1db has no customLocate" );
            }

            /**
             * Return OK if it's not
             * Otherwise return a status that can be displayed
             */
            BSONObj getKey() const {
                // check if valid? also will this work with the _cachedData scoped pointer?
                _load();
                return _cachedKey;
            }

            DiskLoc getDiskLoc() const {
                _load();
                return _cachedLoc;
            }

            // assuming noop if locate hasn't been called
            void advance() {
                if (!_isValid)
                    return;

                Dbt key, value;

                if ( _forward() ) {
                    if ( _cursor->get(&key, &value, DB_NEXT) == DB_NOTFOUND )
                        _isValid = false;
                }
                else {
                    if ( _cursor->get(&key, &value, DB_PREV) == DB_NOTFOUND )
                        _isValid = false;
                }

                _cached = false;
            }

            void savePosition() {
                invariant( !"Berkeley1db cursor doesn't do saving yet" );
            }

            void restorePosition() {
                invariant( !"Berkeley1db cursor doesn't do saving yet" );
            }

        private:

            bool _forward() const { return _direction > 0; }

            void _checkStatus() {
                // todo: Fix me
                
            }
            void _load() const {
                if ( _cached )
                    return;
                _cached = true;
                char* data;
                Dbt key(data, 0);
                key.set_flags(DB_DBT_MALLOC);
                Dbt loc(reinterpret_cast<char *>(&_cachedLoc), sizeof(DiskLoc));
                loc.set_flags(DB_DBT_USERMEM);
                _cursor->get(&key, &loc, DB_CURRENT);
                _cachedData.reset(data);
                _cachedKey = BSONObj( _cachedData.get() );
            }

            // will this giva a memory leak?
            Dbc* _cursor;
            bool _direction;

            mutable bool _cached;
            bool _isValid;
            mutable BSONObj _cachedKey;
            mutable DiskLoc _cachedLoc;
            mutable scoped_ptr<char> _cachedData;
        };

    }

    Berkeley1BtreeImpl::Berkeley1BtreeImpl( DbEnv& env, const std::string& ns, 
        const std::string& indexName )
        : _env( env ), _ns( ns ), _indexName( indexName ), db( &env, 0 ) {
        // Open DB

        std::string db_name = _ns + "." + _indexName + ".db";
        try {
              //TODO set error stream
              //db.set_error_stream(error());

              uint32_t cFlags_ = (DB_CREATE | DB_AUTO_COMMIT | DB_READ_UNCOMMITTED);
              db.set_flags(DB_DUPSORT);
              // Open the database
              db.open(NULL, db_name.data(), NULL, DB_BTREE, cFlags_, 0);
          }

          catch(DbException &e) {
              log() << "Error opening index database: " << db_name << "\n";
              log() << e.what() << std::endl;
          }
          catch(std::exception &e) {
              log() << "Error opening index database: " << db_name << "\n";
              log() << e.what() << std::endl;
          }

    }

    BtreeBuilderInterface* Berkeley1BtreeImpl::getBulkBuilder(OperationContext* txn,
                                                          bool dupsAllowed) {
        invariant( false );
    }

    Status Berkeley1BtreeImpl::insert(OperationContext* txn,
                                  const BSONObj& key,
                                  const DiskLoc& loc,
                                  bool dupsAllowed) {

        Berkeley1RecoveryUnit* ru = _getRecoveryUnit( txn );
        Dbt dbt_key(const_cast<char*>(key.objdata()), key.objsize());
        Dbt value(reinterpret_cast<char*>(const_cast<DiskLoc*>(&loc)), sizeof(DiskLoc));

        if ( !dupsAllowed ) {
            if ( db.put( ru->getCurrentTransaction(), &dbt_key, &value, DB_NODUPDATA) == DB_KEYEXIST )
                return Status(ErrorCodes::BadValue, "Btree Duplicates Not Allowed");
        }
        else {
            invariant(db.put( ru->getCurrentTransaction(), &dbt_key, &value, 0) == 0);
        }

        return Status::OK();
    }

    // assuming returns true on success, 0 on not found
    bool Berkeley1BtreeImpl::unindex(OperationContext* txn,
                                 const BSONObj& key,
                                 const DiskLoc& loc) {
        Berkeley1RecoveryUnit* ru = _getRecoveryUnit( txn );
        Dbt dbt_key(const_cast<char*>(key.objdata()), key.objsize());
        Dbt value(reinterpret_cast<char*>(const_cast<DiskLoc*>(&loc)), sizeof(DiskLoc));

        Dbc* cursor;

        // This may not work since env not initialized with DB_INIT_CDB, however we don't want 
        // this because it only allows 1 writer at a time
        db.cursor(ru->getCurrentTransaction(), &cursor, 0);
        if (cursor->get(&dbt_key, &value, DB_GET_BOTH) == DB_NOTFOUND)
            return false;

        cursor->del(0);
        cursor->close();

        return true; 
    }

    Status Berkeley1BtreeImpl::dupKeyCheck(const BSONObj& key, const DiskLoc& loc) {
        // XXX: not done yet!
        return Status::OK();
    }

    void Berkeley1BtreeImpl::fullValidate(long long* numKeysOut) {
        // XXX: no key counts
        if ( numKeysOut )
            numKeysOut[0] = -1;
    }

    bool Berkeley1BtreeImpl::isEmpty() {
        // XXX: todo
        return false;
    }

    Status Berkeley1BtreeImpl::touch(OperationContext* txn) const {
        // no-op
        return Status::OK();
    }

    BtreeInterface::Cursor* Berkeley1BtreeImpl::newCursor(int direction) const {
        return new Berkeley1Cursor( db, direction );
    }

    Status Berkeley1BtreeImpl::initAsEmpty(OperationContext* txn) {
        // no-op
        return Status::OK();
    }

    Berkeley1RecoveryUnit* Berkeley1BtreeImpl::_getRecoveryUnit( OperationContext* opCtx ) const {
        return dynamic_cast<Berkeley1RecoveryUnit*>( opCtx->recoveryUnit() );
    }

}
