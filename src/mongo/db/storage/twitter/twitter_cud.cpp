// twitter_cud.cpp

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

#include "mongo/db/storage/twitter/twitter_cud.h"

namespace mongo {
	TwitterCUD::TwitterCUD(string username, string password) {/*
		boost::mutex::scoped_lock lk( _curlLock );
		t = twitCurl();
		t.setTwitterUsername(username);
		t.setTwitterPassword(password);

		std::string tmpStr, tmpStr2;
	    std::string replyMsg;
	    char tmpBuf[1024];

		t.getOAuth().setConsumerKey( std::string( "t6PEWWSkw1KAvcyEKiAR4Apv9" ) );
    	t.getOAuth().setConsumerSecret( std::string( "ETmHXdWCo3BZRfuAcypznSywmYnFa2SHidJLYclJt4D2623XK6" ) );

		

	    std::string myOAuthAccessTokenKey("");
	    std::string myOAuthAccessTokenSecret("");
	    std::ifstream oAuthTokenKeyIn;
	    std::ifstream oAuthTokenSecretIn;

	    oAuthTokenKeyIn.open( "twitterClient_token_key.txt" );
	    oAuthTokenSecretIn.open( "twitterClient_token_secret.txt" );

	    memset( tmpBuf, 0, 1024 );
	    oAuthTokenKeyIn >> tmpBuf;
	    myOAuthAccessTokenKey = tmpBuf;

	    memset( tmpBuf, 0, 1024 );
	    oAuthTokenSecretIn >> tmpBuf;
	    myOAuthAccessTokenSecret = tmpBuf;

	    oAuthTokenKeyIn.close();
	    oAuthTokenSecretIn.close();

	    if( myOAuthAccessTokenKey.size() && myOAuthAccessTokenSecret.size() )
	    {
	        
	        printf( "\nUsing:\nKey: %s\nSecret: %s\n\n", myOAuthAccessTokenKey.c_str(), myOAuthAccessTokenSecret.c_str() );

	        t.getOAuth().setOAuthTokenKey( myOAuthAccessTokenKey );
	        t.getOAuth().setOAuthTokenSecret( myOAuthAccessTokenSecret );
	    }
	    else
	    {
	        
	        std::string authUrl;
	        t.oAuthRequestToken( authUrl );

	       
	        memset( tmpBuf, 0, 1024 );
	        printf( "\nDo you want to visit twitter.com for PIN (0 for no; 1 for yes): " );
	        gets( tmpBuf );
	        tmpStr = tmpBuf;
	        if( std::string::npos != tmpStr.find( "1" ) )
	        {
	            
	            memset( tmpBuf, 0, 1024 );
	            printf( "\nPlease visit this link in web browser and authorize this application:\n%s", authUrl.c_str() );
	            printf( "\nEnter the PIN provided by twitter: " );
	            gets( tmpBuf );
	            tmpStr = tmpBuf;
	            t.getOAuth().setOAuthPin( tmpStr );
	        }
	        else
	        {
	            
	            t.oAuthHandlePIN( authUrl );
	        }

	       
	        t.oAuthAccessToken();

	 
	        t.getOAuth().getOAuthTokenKey( myOAuthAccessTokenKey );
	        t.getOAuth().getOAuthTokenSecret( myOAuthAccessTokenSecret );

	    
	        std::ofstream oAuthTokenKeyOut;
	        std::ofstream oAuthTokenSecretOut;

	        oAuthTokenKeyOut.open( "twitterClient_token_key.txt" );
	        oAuthTokenSecretOut.open( "twitterClient_token_secret.txt" );

	        oAuthTokenKeyOut.clear();
	        oAuthTokenSecretOut.clear();

	        oAuthTokenKeyOut << myOAuthAccessTokenKey.c_str();
	        oAuthTokenSecretOut << myOAuthAccessTokenSecret.c_str();

	        oAuthTokenKeyOut.close();
	        oAuthTokenSecretOut.close();
	    }
	    
	    bool b = false;
	    try {
	    	b = t.accountVerifyCredGet();
	    }
	    catch (exception e) {
	    	printf("EXCEPTION\n");
	    	//printf(e.what());
	    	cout << e.what() << std::endl;
	    	fflush(stdout);
	    }
	    if( b )
	    {
	        t.getLastWebResponse( replyMsg );
	        printf( "\ntwitterClient:: twitCurl::accountVerifyCredGet web response:\n%s\n", replyMsg.c_str() );
	    }
	    else
	    {
	        t.getLastCurlError( replyMsg );
	        printf( "\ntwitterClient:: twitCurl::accountVerifyCredGet error:\n%s\n", replyMsg.c_str() );
	    }
	*/
	    
	}

    bool TwitterCUD::insert(BSONObj obj, DiskLoc loc, string ns) {
    	
        string toTweet = _toTweetString(obj, loc, ns);
        if (toTweet.find("local.startup") != std::string::npos)
        	return true;

        system(("./main \"" + toTweet + "\"").c_str());
        return true;
    }

    bool TwitterCUD::remove(DiskLoc loc, string ns) {
        string toTweet;

        toTweet.append(ns);
        toTweet.append(":");
        int ofs = loc.getOfs(), a = loc.a();
        stringstream ss;
		ss << ofs;
        toTweet.append(string(ss.str()));
        toTweet.append(",");
        stringstream ss2;
		ss2 << a;
        toTweet.append(string(ss2.str()));
        toTweet.append(":");
        toTweet.append("Delete");

        system(("./main \"" + toTweet + "\"").c_str());
        return true;
    }

    bool TwitterCUD::custom(string s, string ns) {
    	//boost::mutex::scoped_lock lk( _curlLock );
    	string toTweet = ns + ":" + s;
    	system(("./main \"" + toTweet + "\"").c_str());
    	return true;
    }

    string TwitterCUD::_toTweetString(BSONObj obj, DiskLoc loc, string ns) {
    	string toTweet;
        toTweet.append(ns);
        toTweet.append(":");
        int ofs = loc.getOfs(), a = loc.a();
        stringstream ss;
		ss << ofs;
        toTweet.append(string(ss.str()));
        toTweet.append(",");
        stringstream ss2;
		ss2 << a;
        toTweet.append(string(ss2.str()));
        toTweet.append(":");
        toTweet.append(obj.toString());

        return toTweet;
    }

    TwitterCUD& TwitterCUD::twitterCUD(){
        static TwitterCUD tc = TwitterCUD("trecordstore_0", "mongodb");

        return tc;
    }

} // namespace mongo
