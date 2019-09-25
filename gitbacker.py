#!/usr/bin/env python3

import requests
import json
import logging
import re
import os
try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser
from git import Repo

class GitHub( object ):

    def __init__( self, username, token ):

        self.logger = logging.getLogger( 'github' )
        self.username = username
        self.headers = {'Authorization': 'token {}'.format( token )}

    def _call_api( self, path, relative=True ):
        
        if relative:
            path = 'https://api.github.com/{}'.format( path )

        # Get the API response and decode it from JSON.
        self.logger.info( 'calling {}'.format( path ) )
        r = requests.get( path, headers=self.headers )

        return r.json()

    def get_user( self, username ):
        return self._call_api( 'users/{}'.format( username ) )

    def get_starred( self, username ):
        user = self.get_user( username )
        stars_url = re.sub( r'{.*}', '', user['starred_url'] )
        return self._call_api( stars_url, relative=False )

    def get_repos( self, username ):
        user = self.get_user( username )
        return self._call_api( 'repos/{}'.format( username ) )

if '__main__' == __name__:

    logging.basicConfig( level=logging.INFO )
    logger = logging.getLogger( 'main' )

    config = ConfigParser()
    config.read( 'gitbacker.ini' )    
    username = config.get( 'auth', 'username' )
    api_token = config.get( 'auth', 'token' )
    repo_root = config.get( 'options', 'repo_dir' )

    git = GitHub( username, api_token )
    for repo in git.get_starred( username ):
        logger.info(  '{} ({})'.format( repo['name'], repo['id'] ) )

        repo_dir = os.path.join( repo_root, '{}.git'.format( repo['name'] ) )
        if not os.path.exists( repo_dir ):
            logger.info( 'creating local repo copy...' )
            Repo.clone_from( repo['git_url'], repo_dir, bare=True )
        else:
            logger.info( 'checking remote repo for changes...' )
            r = Repo( repo_dir )
            branches = [b.name for b in r.branches]
            for remote in r.remotes:
                for branch in branches:
                    logger.info( 'checking {} branch: {}'.format(
                        repo['name'], branch ) )
                    remote.fetch( branch )

