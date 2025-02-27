#!/usr/bin/env python

import requests
import json
import logging
import re
import os
import shutil
import subprocess
import sqlite3
import traceback
import signal
import sys
from argparse import ArgumentParser
from smtplib import SMTP
try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser
from git import Repo, Remote
from git.exc import GitCommandError

PATTERN_REMOTE_REF = re.compile( r'.*find remote ref.*' )

class GitBackupFailedException( Exception ):
    def __init__( self, msg, repo_dir, op, branch=None ):
        super( GitBackupFailedException, self ).__init__(
            'ERROR during {}'.format( op ) )
        self.repo_dir = repo_dir
        self.op = op
        self.branch = branch
        self.msg = msg

class GitHubRepo( object ):

    def __init__( self, repo_json, topic_filter=None, max_size=None ):
        self._repo = repo_json
        self.topic_filter = topic_filter
        self.max_size = max_size
        for key in repo_json:
            setattr( self, key, repo_json[key] )
        self.owner = repo_json['owner']['login']
        self.logger = logging.getLogger( 'github.repo' )

    def backup( self, local ):

        working_repo_path = os.path.join( self.owner, self.name )

        # If a topic arg was specified, only backup repos with that topic.
        if self.topic_filter and \
        ('topics' not in repo or self.topic_filter not in self.topics):
            return False

        self.logger.info( '{} ({})'.format( working_repo_path, self.id ) )
        self.logger.info( 'repo size: {}'.format( self.size / 1024 ) )

        # Make sure the repo isn't too big.
        if self.max_size and self.max_size <= (self.size / 1024):
            self.logger.warning(
                'skipping repo {} larger than {} ({})'.format(
                working_repo_path, max_size, self.size ) )
            return False

        # Make sure owner directory exists.
        owner_path = os.path.join( local.get_root(), self.owner )
        if not os.path.exists( owner_path ):
            self.logger.info(
                'creating owner path for {}'.format( self.owner ) )
            os.mkdir( owner_path )

        local.create_or_update( self.name, self.git_url, self.owner )
        local.update_metadata( self )

class GitHubGist( GitHubRepo ):

    def backup( self, local ):

        owner_gist_path = os.path.join( self.owner, self.id )
        self.logger.info( '{}'.format( owner_gist_path ) )

        # Make sure owner directory exists.
        owner_path = os.path.join( local.get_root(), self.owner )
        if not os.path.exists( owner_path ):
            logger.info(
                'creating owner path for {}'.format( self.owner ) )
            os.mkdir( owner_path )

        local.create_or_update( self.id, self.git_pull_url, self.owner )
        local.update_metadata( self )

class GitHub( object ):

    def __init__( self, username, token, topic_filter, max_size, skip_repos ):

        self.logger = logging.getLogger( 'github' )
        self.username = username
        self.headers = { 'Authorization': 'token {}'.format( token ),
            'Accept': 'application/vnd.github.mercy-preview+json' }
        self.topic_filter = topic_filter
        self.max_size = max_size
        self.skip_repos = skip_repos

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
            yield GitHubRepo( repo, self.topic_filter, self.max_size )

    def get_own_user_repos( self ):
        response = self._call_api( 'user/repos' )
        for repo in self._get_paged( response ):
            repo_full = '{}/{}'.format( repo['owner']['login'], repo['name'] )
            if repo_full in self.skip_repos:
                self.logger.info( 'skipping repo %s...', repo_full )
                continue
            
            yield GitHubRepo( repo, self.topic_filter, self.max_size )

    def get_own_starred_gists( self ):
        response = self._call_api( 'gists/starred' )
        for gist in self._get_paged( response ):
            yield GitHubGist( gist, self.topic_filter, self.max_size )

    def get_user_gists( self, username ):
        user = self.get_user( username )
        response = self._call_api( 'users/{}/gists'.format( username ) )
        for gist in self._get_paged( response ):
            yield GitHubGist( gist, self.topic_filter, self.max_size )

class LocalRepo( object ):

    def __init__( self, root, db_conn ):

        self._root = root
        self._db_conn = db_conn
        self.logger = logging.getLogger( 'localrepo' )

    def get_root( self ):
        return self._root

    def get_path( self, repo_name, owner_name=None ):
        if owner_name:
            return os.path.join(
                self._root, owner_name, '{}.git'.format( repo_name ) )
        else:
            return os.path.join( self._root, '{}.git'.format( repo_name ) )

    def fetch_branch( self, owner_name, repo, branch ):

        try_count = 3
        while 0 < try_count:
            self.logger.info( 'checking {}/{} branch: {}'.format(
                owner_name, repo.name, branch ) )
            try:
                remote.fetch( branch )
                # If successful, don't loop.
                try_count = 0
            except Exception as e:
                if PATTERN_REMOTE_REF.search( str( e ) ):
                    # TODO: Maybe use a different logger?
                    self.logger.debug( '{}: {}'.format( repo.name, e ) )
                    # If this is a dead branch, just move on.
                    try_count = 0
                else:
                    try_count -= 1
                    if 0 >= try_count:
                        # Just give up for now.
                        raise GitBackupFailedException(
                            str( e ), repo_dir, 'fetch', branch )

    def fetch_all_branches( self, owner_name, repo_name ):
        repo_dir = self.get_path( repo_name, owner_name )
        repo = Repo( repo_dir )
        branches = []
        try:
            branches = [b.name for b in repo.branches]
        except UnicodeDecodeError as e:
            self.logger.error( 'could not decode branch name: {}'.format( e ) )
        for remote in repo.remotes:
            for branch in branches:
                self.fetch_branch( owner_name, repo, branch )

    def update_server_info( self, owner_name, repo_name ):
        self.logger.info( 'updating server info...' )
        repo_dir = self.get_path( repo_name, owner_name )
        cmd = ['git', 'update-server-info']
        proc = subprocess.Popen( cmd, cwd=repo_dir, stdout=subprocess.PIPE )
        git_std = proc.communicate()
        for line in git_std:
            if line:
                logger.info( '{}: {}'.format( repo_name, line.strip() ) )

    def update_metadata( self, repo ):

        if not self._db_conn:
            return

        cur = self._db_conn.cursor()
        cur.execute(
            'INSERT INTO repos(owner, name, repo_id, topics, desc) ' +
            'VALUES (?, ?, ?, ?, ?)',
            (repo.owner, repo.name, repo.id,
                str( repo.topics ), repo.description) )
        self._db_conn.commit()

    def create_or_update( self, repo_name, remote_url, owner_name=None ):
        repo_dir = self.get_path( repo_name, owner_name )
        try_count = 3

        if not os.path.exists( repo_dir ):
            while 0 < try_count:
                try:
                    self.logger.info( 'creating local repo copy...' )
                    # So our stored credentials work.
                    remote_url = remote_url.replace( 'git://', 'https://' )
                    Repo.clone_from( remote_url, repo_dir, bare=True )
                    self.update_server_info( owner_name, repo_name )
                    try_count = 0
                except GitCommandError as e:
                    self.logger.error( 'error cloning; retrying...' )
                    try_count -= 1
                    if os.path.exists( repo_dir ):
                        logger.debug( 'removing failed clone...' )
                        shutil.rmtree( repo_dir )
                    if 0 >= try_count:
                        raise GitBackupFailedException(
                            str( e ), repo_dir, 'clone' )

        self.logger.info( 'checking all remote repo branches...' )
        self.fetch_all_branches( owner_name, repo_name )
        self.update_server_info( owner_name, repo_name )

class Notifier( object ):

    def __init__( self, host, to_addr, from_addr ):
        self.host = host
        self.to_addr = to_addr
        self.from_addr = from_addr

    def send( self, subject, body ):

        msg = ('From {}\r\nTo: {}\r\nSubject: {}\r\n\r\n{}'.format(
            self.from_addr, self.to_addr, subject, body ) )

        smtp = SMTP( self.host )
        smtp.sendmail( self.from_addr, [self.to_addr], msg )
        smtp.quit()

    def send_exc( self, subject, e ):

        self.send( subject, traceback.format_exc() )

class SigWatcher( object ):
    def __init__( self, notifier, force=False ):
        self.notifier = notifier
        self.force = force
        self.running = True

    def handle( self, signum, frame ):
        self.notifier.send(
            '[gitbacker] Received Signal {}'.format( signum ), '' )
        self.running = False
        if self.force:
            sys.exit( 1 )

def debug_print( struct ):

    from pprint import PrettyPrinter
    pp = PrettyPrinter()
    pp.pprint( struct )

def backup_user_repos( git, local, redo, uname, uemail, notifier, watcher ):

    ''' Backup all repos for the github user this script is accessing the
    API as, to the directory repo_dir/user_name. '''

    count = 0
    logger = logging.getLogger( 'repos.user' )
    for repo in git.get_own_user_repos():

        repo_dir = local.get_path( repo.name )
        if os.path.exists( repo_dir ) and redo:
            # Remove the repo dir so it can be re-created.
            shutil.rmtree( repo_dir )

        try:
            res = repo.backup( local )
        except GitBackupFailedException as e:
            notifier.send_exc(
                '[gitbacker] ERROR during user repo {}'.format( e.op ),
                'msg: {}, repo dir: {}, branch: {}'.format(
                    e.msg, e.repo_dir, e.branch ) )
            continue

        # Change author/committer ID information if specified.
        if res and uname and uemail:
            cmd = ['git', 'filter-branch', '-f', '--env-filter', "GIT_AUTHOR_NAME='{}'; GIT_AUTHOR_EMAIL='{}'; GIT_COMMITTER_NAME='{}'; GIT_COMMITTER_EMAIL='{}';".format( uname, uemail, uname, uemail ), '--', '--all']
            proc = subprocess.Popen( cmd, cwd=repo_dir, stdout=subprocess.PIPE )

            git_std = proc.communicate()
            for line in git_std:
                if line:
                    logger.info( '{}: {}'.format( repo.name, line.strip() ) )

            # Prune all remotes to sterilize.
            r = Repo( repo_dir )
            for remote in r.remotes:
                Remote.remove( r, remote.name )

        count += 1

        if not watcher.running:
            return count

    return count

def backup_starred_repos( git, local, username, notifier, watcher ):
    count = 0
    logger = logging.getLogger( 'repos.starred' )
    for repo in git.get_starred_repos( username ):
        try:
            repo.backup( local )
            count += 1
        except GitBackupFailedException as e:
            notifier.send_exc(
                '[gitbacker] ERROR during starred repo {}'.format( e.op ),
                'msg: {}, repo dir: {}, branch: {}'.format(
                    e.msg, e.repo_dir, e.branch ) )
        if not watcher.running:
            return count
    return count

def backup_user_gists( git, local, username, notifier, watcher ):
    count = 0
    logger = logging.getLogger( 'gists.user' )
    for gist in git.get_user_gists( username ):
        try:
            gist.backup( local )
            count += 1
        except GitBackupFailedException as e:
            notifier.send_exc(
                '[gitbacker] ERROR during user gist {}'.format( e.op ),
                'msg: {}, repo dir: {}, branch: {}'.format(
                    e.msg, e.repo_dir, e.branch ) )
        if not watcher.running:
            return count
    return count

def backup_starred_gists( git, local, notifier, watcher ):
    count = 0
    logger = logging.getLogger( 'gists.starred' )
    for gist in git.get_own_starred_gists():
        try:
            gist.backup( local )
            count += 1
        except GitBackupFailedException as e:
            notifier.send_exc(
                '[gitbacker] ERROR during starred gist {}'.format( e.op ),
                'msg: {}, repo dir: {}, branch: {}'.format(
                    e.msg, e.repo_dir, e.branch ) )
        if not watcher.running:
            return count
    return count

def backup_all( git, local, username, args, notifier ):

    error_cond = False
    repos_count = 0
    redo = False
    watcher = SigWatcher( notifier, True ) # TODO: Don't force unless requested.
    signal.signal( signal.SIGINT, watcher.handle )
    signal.signal( signal.SIGTERM, watcher.handle )

    if args.name or args.email or args.redo:
        logger.info( 'Redo enabled.' )
        redo = True

    if args.starred_repos:
        try:
            repos_count += backup_starred_repos(
                git, local, username, notifier, watcher )
        except Exception as e:
            error_cond = True
            notifier.send_exc( '[gitbacker] ERROR during starred_repos', e )
            logger.exception( e )

        if not watcher.running:
            return

    if args.user_repos:
        try:
            repos_count += backup_user_repos(
                git, local, redo, args.name, args.email, notifier, watcher )
        except Exception as e:
            error_cond = True
            notifier.send_exc( '[gitbacker] ERROR during user_repos', e )
            logger.exception( e )

        if not watcher.running:
            return

    if args.user_gists:
        try:
            repos_count += backup_user_gists(
                git, local, username, notifier, watcher )
        except Exception as e:
            error_cond = True
            notifier.send_exc( '[gitbacker] ERROR during user_gists', e )
            logger.exception( e )

        if not watcher.running:
            return

    if args.starred_gists:
        try:
            repos_count += backup_starred_gists( git, local, notifier, watcher )
        except Exception as e:
            error_cond = True
            notifier.send_exc( '[gitbacker] ERROR during starred_gists', e )
            logger.exception( e )

        if not watcher.running:
            return

    if not error_cond:
        notifier.send(
            '[gitbacker] Backed up {} repos OK'.format( repos_count ),
            'Backed up {} repos OK'.format( repos_count ) )

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
    parser.add_argument( '-d', '--db', action='store_true',
        help='Store metadata in DB from config.' )

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
    db_path = config.get( 'options', 'db_path' )
    skip = config.get( 'options', 'skip' )
    notifier = Notifier(
        config.get( 'notify', 'smtp_host' ),
        config.get( 'notify', 'smtp_to' ),
        config.get( 'notify', 'smtp_from' ) )
    git = GitHub( username, api_token, args.topic, args.max_size, skip )

    if args.db:
        with sqlite3.connect( db_path ) as db_conn:

            # Setup the database.
            cur = db_conn.cursor()
            cur.execute( '''CREATE TABLE IF NOT EXISTS repos (
                id INTEGER PRIMARY KEY,
                owner TEXT NOT NULL,
                name TEXT NOT NULL,
                repo_id TEXT NOT NULL,
                topics TEXT,
                desc TEXT)
            ''' )
            db_conn.commit()

            local = LocalRepo( config.get( 'options', 'repo_dir' ), db_conn )
            backup_all( git, local, username, args, notifier )
    else:
        # Don't use a DB connection.
        local = LocalRepo( config.get( 'options', 'repo_dir' ), None )
        backup_all( git, local, username, args, notifier )

