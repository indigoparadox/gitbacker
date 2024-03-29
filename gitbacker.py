#!/usr/bin/env python

import requests
import json
import logging
import re
import os
import shutil
import subprocess
from argparse import ArgumentParser
try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser
from git import Repo, Remote

class GitHub( object ):

    def __init__( self, username, token ):

        self.logger = logging.getLogger( 'github' )
        self.username = username
        self.headers = { 'Authorization': 'token {}'.format( token ),
            'Accept': 'application/vnd.github.mercy-preview+json' }

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
                    return {'json': r.json(), 'next': link['url']}

        return {'json': r.json(), 'next': None}

    def get_user( self, username ):
        res = self._call_api( 'users/{}'.format( username ) )['json']
        self.logger.debug( res )
        return res

    def _get_paged( self, response ):
        for repo in response['json']:
            yield repo
        while None != response['next']:
            response = self._call_api( response['next'], relative=False )
            for repo in response['json']:
                yield repo

    def get_starred_repos( self, username ):
        user = self.get_user( username )
        stars_url = re.sub( r'{.*}', '', user['starred_url'] )
        response = self._call_api( stars_url, relative=False )
        for repo in self._get_paged( response ):
            yield repo

    def get_own_user_repos( self ):
        response = self._call_api( 'user/repos' )
        for repo in self._get_paged( response ):
            yield repo

    def get_own_starred_gists( self ):
        response = self._call_api( 'gists/starred' )
        for gist in self._get_paged( response ):
            yield gist

    def get_user_gists( self, username ):
        user = self.get_user( username )
        response = self._call_api( 'users/{}/gists'.format( username ) )
        for gist in self._get_paged( response ):
            yield gist

class LocalRepo( object ):

    def __init__( self, root ):

        self._root = root
        self.logger = logging.getLogger( 'localrepo' )

    def get_root( self ):
        return self._root

    def get_path( self, repo, owner=None ):
        if owner:
            return os.path.join( self._root, owner, '{}.git'.format( repo ) )
        else:
            return os.path.join( self._root, '{}.git'.format( repo ) )

    def fetch_all_branches( self, owner, repo ):
        repo_dir = self.get_path( repo, owner )
        r = Repo( repo_dir )
        branches = []
        try:
            branches = [b.name for b in r.branches]
        except UnicodeDecodeError as e:
            self.logger.error( 'could not decode branch name: {}'.format( e ) )
        for remote in r.remotes:
            for branch in branches:
                self.logger.info( 'checking {}/{} branch: {}'.format(
                    owner, repo, branch ) )
                try:
                    remote.fetch( branch )
                except Exception as e:
                    self.logger.error( '{}: {}'.format( repo, e ) )

    def create_or_update( self, repo, remote_url, owner=None ):
        repo_dir = self.get_path( repo, owner )

        if not os.path.exists( repo_dir ):
            self.logger.info( 'creating local repo copy...' )
            # So our stored credentials work.
            remote_url = remote_url.replace( 'git://', 'https://' )
            Repo.clone_from( remote_url, repo_dir, bare=True )

        self.logger.info( 'checking all remote repo branches...' )
        self.fetch_all_branches( owner, repo )

        # TODO: Handle failures.
        return True

def debug_print( struct ):

    from pprint import PrettyPrinter
    pp = PrettyPrinter()
    pp.pprint( struct )

def backup_repo( local, repo, logger, max_size, topic, owner=None ):

    if owner:
        working_repo_path = os.path.join( owner, repo['name'] )
    else:
        working_repo_path = repo['name']

    # Use topic if available.
    if topic and ('topics' not in repo or topic not in repo['topics']):
        return False

    logger.info( '{} ({})'.format( working_repo_path, repo['id'] ) )
    logger.info( 'repo size: {}'.format( repo['size'] / 1024 ) )

    # Make sure the repo isn't too big.
    if max_size and max_size <= (repo['size'] / 1024):
        logger.warning( 'skipping repo {} larger than {} ({})'.format(
            working_repo_path, max_size, repo['size'] ) )
        return False

    # Make sure owner directory exists.
    if owner:
        owner_path = os.path.join( local.get_root(), owner )
        if not os.path.exists( owner_path ):
            logger.info( 'creating owner path for {}'.format( owner ) )
            os.mkdir( owner_path )

    return local.create_or_update( repo['name'], repo['git_url'], owner)

def backup_gist( local, gist, logger ):

    owner_gist_path = os.path.join( gist['owner']['login'], gist['id'] )
    logger.info( '{}'.format( owner_gist_path ) )

    # Make sure owner directory exists.
    owner_path = os.path.join( local.get_root(), gist['owner']['login'] )
    if not os.path.exists( owner_path ):
        logger.info(
            'creating owner path for {}'.format( gist['owner']['login'] ) )
        os.mkdir( owner_path )

    local.create_or_update(
        gist['id'], gist['git_pull_url'], gist['owner']['login'] )

def backup_user_repos( git, local, max_size, topic, udir, redo, uname, uemail ):
    logger = logging.getLogger( 'user-repos' )
    for repo in git.get_own_user_repos():

        repo_dir = local.get_path( repo['name'] )
        if udir:
            repo_dir = local.get_path( repo, repo['owner']['login'] )

        if os.path.exists( repo_dir ) and redo:
            # Remove the repo dir so it can be re-created.
            shutil.rmtree( repo_dir )

        res = backup_repo( local, repo, logger, max_size, topic,
            repo['owner']['login'] if udir else None )

        # Change author/committer ID information if specified.
        if res and uname and uemail:
            cmd = ['git', 'filter-branch', '-f', '--env-filter', "GIT_AUTHOR_NAME='{}'; GIT_AUTHOR_EMAIL='{}'; GIT_COMMITTER_NAME='{}'; GIT_COMMITTER_EMAIL='{}';".format( uname, uemail, uname, uemail ), '--', '--all']
            proc = subprocess.Popen( cmd, cwd=repo_dir, stdout=subprocess.PIPE )

            git_std = proc.communicate()
            for line in git_std:
                if line:
                    logger.info( '{}: {}'.format( repo['name'], line.strip() ) )

            # Prune all remotes to sterilize.
            r = Repo( repo_dir )
            for remote in r.remotes:
                Remote.remove( r, remote.name )
            

def backup_starred_repos( git, local, username, max_size, topic ):
    logger = logging.getLogger( 'starred-repos' )
    for repo in git.get_starred_repos( username ):
        backup_repo(
            local, repo, logger, max_size, topic, repo['owner']['login'] )

def backup_user_gists( git, local, username ):
    logger = logging.getLogger( 'user-gists' )
    for gist in git.get_user_gists( username ):
        backup_gist( local, gist, logger )

def backup_starred_gists( git, local ):
    logger = logging.getLogger( 'starred-gists' )
    for gist in git.get_own_starred_gists():
        backup_gist( local, gist, logger )

if '__main__' == __name__:

    # Parse CLI args.
    parser = ArgumentParser()

    parser.add_argument( '-c', '--config', default='gitbacker.ini',
        help='Path to the config file to load.' )
    parser.add_argument( '-q', '--quiet', action='store_true',
        help='Quiet mode.' )
    parser.add_argument( '-v', '--verbose', action='store_true',
        help='Verbose mode.' )
    parser.add_argument( '-s', '--starred-repos', action='store_true',
        help='Backup starred repositories.' )
    parser.add_argument( '-r', '--user-repos', action='store_true',
        help='Backup user repositories.' )
    parser.add_argument( '-m', '--max-size', type=int,
        help='Maximum repo size. Ignore repos larger than in MB.' )
    parser.add_argument( '-g', '--user-gists', action='store_true',
        help='Backup user gists.' )
    parser.add_argument( '-f', '--starred-gists', action='store_true',
        help='Backup authenticated user\'s starred gists.' )
    parser.add_argument( '-t', '--topic', action='store',
        help='Only backup repositories with the given topic attached.' )
    parser.add_argument( '-x', '--redo', action='store_true',
        help='Remove existing repos and re-clone.' )
    parser.add_argument( '-e', '--email', action='store',
        help='Change the e-mail on commits to downloaded repos (implies -x).' )
    parser.add_argument( '-n', '--name', action='store',
        help='Change the name on commits to downloaded repos (implies -x).' )
    parser.add_argument( '-w', '--without-user', action='store',
        help='Do not place repos in user subdirectory.' )

    args = parser.parse_args()

    if args.quiet:
        logging.basicConfig( level=logging.WARNING )
    elif args.verbose:
        logging.basicConfig( level=logging.DEBUG )
    else:
        logging.basicConfig( level=logging.INFO )
    logger = logging.getLogger( 'main' )

    # Load auth config.
    config = ConfigParser()
    config.read( args.config )
    username = config.get( 'auth', 'username' )
    api_token = config.get( 'auth', 'token' )
    redo = False

    git = GitHub( username, api_token )
    local = LocalRepo( config.get( 'options', 'repo_dir' ) )

    if args.name or args.email or args.redo:
        logger.info( 'Redo enabled.' )
        redo = True

    if args.starred_repos:
        backup_starred_repos( git, local, username, args.max_size, args.topic )

    if args.user_repos:
        backup_user_repos(
            git, local, args.max_size, args.topic, args.without_user,
            redo, args.name, args.email )

    if args.user_gists:
        backup_user_gists( git, local, username )

    if args.starred_gists:
        backup_starred_gists( git, local )

