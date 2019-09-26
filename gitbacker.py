#!/usr/bin/env python

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

class LocalRepo( object ):

    def __init__( self, root ):

        self.root = root
        self.logger = logging.getLogger( 'localrepo' )

    def get_path( self, repo ):
        return os.path.join( self.root, '{}.git'.format( repo ) )

    def fetch_all_branches( self, repo ):
        repo_dir = self.get_path( repo )
        r = Repo( repo_dir )
        branches = [b.name for b in r.branches]
        for remote in r.remotes:
            for branch in branches:
                self.logger.info( 'checking {} branch: {}'.format(
                    repo, branch ) )
                remote.fetch( branch )

    def create_or_update( self, repo, remote_url ):
        repo_dir = self.get_path( repo )
        if not os.path.exists( repo_dir ):
            self.logger.info( 'creating local repo copy...' )
            Repo.clone_from( remote_url, repo_dir, bare=True )
        else:
            self.logger.info( 'checking remote repo for changes...' )
            self.fetch_all_branches( repo )

if '__main__' == __name__:

    logging.basicConfig( level=logging.INFO )
    logger = logging.getLogger( 'main' )

    config = ConfigParser()
    config.read( 'gitbacker.ini' )    
    username = config.get( 'auth', 'username' )
    api_token = config.get( 'auth', 'token' )

    git = GitHub( username, api_token )
    local = LocalRepo( config.get( 'options', 'repo_dir' ) )
    for repo in git.get_starred( username ):
        logger.info(  '{} ({})'.format( repo['name'], repo['id'] ) )

        local.create_or_update( repo['name'], repo['git_url'] )

