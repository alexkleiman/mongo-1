// twitter_btree_impl.cpp

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

#include "mongo/db/storage/twitter/twitter_btree_impl.h"

#include <string>

#include "mongo/db/storage/index_entry_comparison.h"
#include "mongo/db/storage/twitter/twitter_engine.h"
#include "mongo/db/storage/twitter/twitter_recovery_unit.h"

namespace mongo {

    namespace {

        class TwitterCursor : public BtreeInterface::Cursor {
        public:
            TwitterCursor( bool direction ): _direction( direction ) {

                    invariant(!"nyi");
            }

            virtual ~TwitterCursor() {}

            int getDirection() const { return _direction; }

            bool isEOF() const {
                invariant(!"nyi");
            }

            /**
             * Will only be called with other from same index as this.
             * All EOF locs should be considered equal.
             */
            bool pointsToSamePlaceAs(const Cursor& other) const {
                invariant(!"nyi");
            }

            void aboutToDeleteBucket(const DiskLoc& bucket) {
                // don't know if I need this or not
            }

            bool locate(const BSONObj& key, const DiskLoc& loc) {
                invariant(!"nyi");
            }

            void advanceTo(const BSONObj &keyBegin,
                           int keyBeginLen,
                           bool afterKey,
                           const vector<const BSONElement*>& keyEnd,
                           const vector<bool>& keyEndInclusive) {
                invariant( !"twitterdb has no advanceTo" );
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
                invariant( !"twitterdb has no customLocate" );
            }

            /**
             * Return OK if it's not
             * Otherwise return a status that can be displayed
             */
            BSONObj getKey() const {
                invariant(!"nyi");
            }

            DiskLoc getDiskLoc() const {
                invariant(!"nyi");
            }

            void advance() {
                invariant(!"nyi");
            }

            void savePosition() {
                invariant( !"twitterdb cursor doesn't do saving yet" );
            }

            void restorePosition() {
                invariant( !"twitterdb cursor doesn't do saving yet" );
            }

        private:
            bool _direction;

        };

    }

    TwitterBtreeImpl::TwitterBtreeImpl() {
       invariant(!"nyi"); 
    }

    BtreeBuilderInterface* TwitterBtreeImpl::getBulkBuilder(OperationContext* txn,
                                                          bool dupsAllowed) {
        invariant( false );
    }

    Status TwitterBtreeImpl::insert(OperationContext* txn,
                                  const BSONObj& key,
                                  const DiskLoc& loc,
                                  bool dupsAllowed) {

        invariant(!"nyi");
        return Status::OK();
    }

    bool TwitterBtreeImpl::unindex(OperationContext* txn,
                                 const BSONObj& key,
                                 const DiskLoc& loc) {
        invariant(!"nyi");
        return 1; // XXX: fix? does it matter since its so slow to check?
    }

    Status TwitterBtreeImpl::dupKeyCheck(OperationContext* txn, const BSONObj& key, const DiskLoc& loc) {
        // XXX: not done yet!
        return Status::OK();
    }

    void TwitterBtreeImpl::fullValidate(OperationContext* txn, long long* numKeysOut) {
        invariant(!"nyi");
    }

    bool TwitterBtreeImpl::isEmpty() {
        invariant(!"nyi");
        return false;
    }

    Status TwitterBtreeImpl::touch(OperationContext* txn) const {
        // no-op
        return Status::OK();
    }

    BtreeInterface::Cursor* TwitterBtreeImpl::newCursor(OperationContext* txn,
                                                      int direction) const {
        invariant(!"nyi");
        return NULL;
    }

    Status TwitterBtreeImpl::initAsEmpty(OperationContext* txn) {
        // no-op
        return Status::OK();
    }

    TwitterRecoveryUnit* TwitterBtreeImpl::_getRecoveryUnit( OperationContext* opCtx ) const {
        return dynamic_cast<TwitterRecoveryUnit*>( opCtx->recoveryUnit() );
    }

}
