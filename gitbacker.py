#!/usr/bin/env python

import requests
import json
import logging
import re
import os
from argparse import ArgumentParser
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

        self._root = root
        self.logger = logging.getLogger( 'localrepo' )

    def get_root( self ):
        return self._root

    def get_path( self, owner, repo ):
        return os.path.join( self._root, owner, '{}.git'.format( repo ) )

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

def backup_repo( local, repo, logger ):

    owner_repo_path = os.path.join( repo['owner']['login'], repo['name'] )
    logger.info( '{} ({})'.format( owner_repo_path, repo['id'] ) )
    logger.info( 'repo size: {}'.format( repo['size'] / 1024 ) )

    #from pprint import PrettyPrinter
    #pp = PrettyPrinter()
    #pp.pprint( repo )

    # Make sure owner directory exists.
    owner_path = os.path.join( local.get_root(), repo['owner']['login'] )
    if not os.path.exists( owner_path ):
        logger.info(
            'creating owner path for {}'.format( repo['owner']['login'] ) )
        os.mkdir( owner_path )

    local.create_or_update(
        repo['owner']['login'], repo['name'], repo['git_url'] )

def backup_user_repos( git, local, username, max_size ):
    logger = logging.getLogger( 'user' )
    for repo in git.get_user_repos( username ):
        if max_size <= (repo['size'] / 1024):
            logger.warning( 'skipping repo {}/{} larger than {} ({})'.format(
                repo['owner']['login'], repo['name'], max_size, repo['size'] ) )
            continue
        backup_repo( local, repo, logger )

def backup_starred( git, local, username, max_size ):
    logger = logging.getLogger( 'starred' )
    for repo in git.get_starred( username ):
        if max_size <= repo['size']:
            logger.warning( 'skipping repo {}/{} larger than {} ({})'.format(
                repo['owner']['login'], repo['name'], max_size, repo['size'] ) )
            continue
        backup_repo( local, repo, logger )

if '__main__' == __name__:

    # Parse CLI args.
    parser = ArgumentParser()

    parser.add_argument( '-c', '--config', default='gitbacker.ini',
        help='Path to the config file to load.' )
    parser.add_argument( '-q', '--quiet', action='store_true',
        help='Quiet mode.' )
    parser.add_argument( '-s', '--starred', action='store_true',
        help='Backup starred repositories.' )
    parser.add_argument( '-r', '--repos', action='store_true',
        help='Backup user repositories.' )
    parser.add_argument( '-m', '--max-size', type=int,
        help='Maximum repo size. Ignore repos larger than in MB.' )

    args = parser.parse_args()

    if args.quiet:
        logging.basicConfig( level=logging.WARNING )
    else:
        logging.basicConfig( level=logging.INFO )
    logger = logging.getLogger( 'main' )

    # Load auth config.
    config = ConfigParser()
    config.read( args.config )
    username = config.get( 'auth', 'username' )
    api_token = config.get( 'auth', 'token' )

    git = GitHub( username, api_token )
    local = LocalRepo( config.get( 'options', 'repo_dir' ) )

    if args.starred:
        backup_starred( git, local, username, args.max_size )

    if args.repos:
        backup_user_repos( git, local, username, args.max_size )

