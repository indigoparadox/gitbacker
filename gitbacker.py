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

        # Parse links if available.
        if 'link' in r.headers:
            rel_links = requests.utils.parse_header_links( r.headers['link'] )
            for link in rel_links:
                if 'next' == link['rel']:
                    return r.json(), link['url']

        return r.json(), None

    def get_user( self, username ):
        return self._call_api( 'users/{}'.format( username ) )[0]

    def get_starred( self, username ):
        user = self.get_user( username )
        stars_url = re.sub( r'{.*}', '', user['starred_url'] )
        response = self._call_api( stars_url, relative=False )
        for repo in response[0]:
            yield repo
        while None != response[1]:
            response = self._call_api( response[1], relative=False )
            for repo in response[0]:
                yield repo

    def get_repos( self, username ):
        user = self.get_user( username )
        return self._call_api( 'repos/{}'.format( username ) )[0]

class LocalRepo( object ):

    def __init__( self, root ):

        self.root = root
        self.logger = logging.getLogger( 'localrepo' )

    def get_path( self, owner, repo ):
        return os.path.join( self.root, owner, '{}.git'.format( repo ) )

    def fetch_all_branches( self, owner, repo ):
        repo_dir = self.get_path( owner, repo )
        r = Repo( repo_dir )
        branches = [b.name for b in r.branches]
        for remote in r.remotes:
            for branch in branches:
                self.logger.info( 'checking {}/{} branch: {}'.format(
                    owner, repo, branch ) )
                remote.fetch( branch )

    def create_or_update( self, owner, repo, remote_url ):
        repo_dir = self.get_path( owner, repo )
        if not os.path.exists( repo_dir ):
            self.logger.info( 'creating local repo copy...' )
            Repo.clone_from( remote_url, repo_dir, bare=True )

        self.logger.info( 'checking all remote repo branches...' )
        self.fetch_all_branches( owner, repo )

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
        owner_repo_path = os.path.join( repo['owner']['login'], repo['name'] )
        logger.info( '{} ({})'.format( owner_repo_path, repo['id'] ) )

        owner_path = os.path.join( config.get( 'options', 'repo_dir' ),
            repo['owner']['login'] )
        if not os.path.exists( owner_path ):
            logger.info(
                'creating owner path for {}'.format( repo['owner']['login'] ) )
            os.mkdir( owner_path )

        local.create_or_update(
            repo['owner']['login'], repo['name'], repo['git_url'] )

