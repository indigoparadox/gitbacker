#!/usr/bin/env python

import json
import logging
import re
import os
import subprocess
import sqlite3
import traceback
import signal
import sys
import multiprocessing
from argparse import ArgumentParser
from smtplib import SMTP
try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser

from . import GitBackupFailedException
from .sources.github import GitHub
from .targets.local import LocalRepo

PATTERN_REMOTE_REF = re.compile( r'.*find remote ref.*' )

MP_MSG_COUNT = 1
MP_MSG_ERRS = 2

class Notifier( object ):

    def __init__( self, host, to_addr, from_addr ):
        self.host = host
        self.to_addr = to_addr
        self.from_addr = from_addr
        self.e_list = []

    def send( self, subject, body ):

        msg = ('From {}\r\nTo: {}\r\nSubject: {}\r\n\r\n{}'.format(
            self.from_addr, self.to_addr, subject, body ) )

        smtp = SMTP( self.host )
        smtp.sendmail( self.from_addr, [self.to_addr], msg )
        smtp.quit()

    def send_exc( self, subject, e ):

        #self.send( subject, e + '\n\n\n' + traceback.format_exc() )
        self.send( subject, e )

    def queue_exc( self, e ):
        self.e_list.append( e )

    def send_queued( self, res_queue ):
        if len( self.e_list ) > 0:
            res_queue.put( (MP_MSG_ERRS, '\n\n'.join( self.e_list) ) )
            #self.send(
            #    '[gitbacker] ERRORs during gitbacker',
            #    '\n\n'.join( self.e_list ) )

class SigWatcher( object ):
    def __init__( self, notifier, msg_queue, force=False ):
        self.notifier = notifier
        self.force = force
        self.running = True
        self.procs = []
        self.msg_queue = msg_queue

    def add_proc( self, proc ):
        self.procs.append( proc )

    def handle( self, signum, frame ):
        #self.notifier.send(
        #    '[gitbacker] Received Signal {}'.format( signum ), '' )
        self.running = False
        for p in self.procs:
            self.msg_queue.put( 'quit' )
        #if self.force:
        #    for p in self.procs:
        #        p.terminate()
        #    sys.exit( 1 )

# TODO: Make a function for this.
#        # Change author/committer ID information if specified.
#        #if res and uname and uemail:
#        #    cmd = ['git', 'filter-branch', '-f', '--env-filter', "GIT_AUTHOR_NAME='{}'; GIT_AUTHOR_EMAIL='{}'; GIT_COMMITTER_NAME='{}'; GIT_COMMITTER_EMAIL='{}';".format( uname, uemail, uname, uemail ), '--', '--all']
#        #    proc = subprocess.Popen( cmd, cwd=repo_dir, stdout=subprocess.PIPE )
#
#        #    git_std = proc.communicate()
#        #    for line in git_std:
#        #        if line:
#        #            logger.info( '{}: {}'.format( repo.name, line.strip() ) )
#
#        #    # Prune all remotes to sterilize.
#        #    r = Repo( repo_dir )
#        #    for remote in r.remotes:
#        #        Remote.remove( r, remote.name )

def backup_repos( div, step, local, username, notifier, msg_queue, fetcher ):
    count_processed = 0
    count_success = 0
    logger = logging.getLogger( 'backup.repos' )
    for repo in fetcher( username ):

        # Allocate by process/thread.
        count_processed += 1
        if step != (count_processed % div):
            # Not this thread's business.
            logger.debug(
                'skipping repo %s on thread %d...', repo.name, os.getpid() )
            continue

        # Perform the backup.
        try:
            repo.backup( local )
            count_success += 1
        except GitBackupFailedException as e:
            notifier.queue_exc(
                'msg: {}, op: {}, args: {}'.format(
                    e.msg, e.op, str( e.func_args ) ) )

        if not msg_queue.empty():
            # Assume the only thing in the queue would be a quit signal.
            logger.info( 'received quit message!' )
            return count_success
    return count_success

def do_backup( config, notifier, res_queue, msg_queue, div, step, kwargs ):

    logger = logging.getLogger( 'backup' )

    username = config.get( 'auth', 'username' )
    api_token = config.get( 'auth', 'token' )
    skip = config.get( 'options', 'skip' )
    db_path = config.get( 'options', 'db_path' )
    git = GitHub(
        username, api_token, kwargs['topic'], kwargs['max_size'], skip )

    repos_count = 0
    db_conn = None
    try:
        if kwargs['db']:
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
        err_list = []

        if kwargs['starred_repos']:
            repos_count += backup_repos(
                div, step,
                local, username, notifier, msg_queue, git.get_starred_repos )

        if kwargs['user_repos']:
            repos_count += backup_repos(
                div, step,
                local, kwargs['name'],
                notifier, msg_queue, git.get_own_user_repos )

        if kwargs['user_gists']:
            repos_count += backup_repos(
                div, step,
                git, local, username, notifier, msg_queue, git.get_user_gists )

        if kwargs['starred_gists']:
            repos_count += backup_repos(
                div, step, git, local, username, notifier, msg_queue,
                git.get_own_starred_gists )

    finally:
        # Close the database if it was opened.
        if db_conn:
            db_conn.close()

        notifier.send_queued( res_queue )

        res_queue.put( (MP_MSG_COUNT, repos_count) )

def do_metaref( config, notifier, **kwargs ):

    local = LocalRepo( config.get( 'options', 'repo_dir' ), None )
    
    for repo_dir, user, repo in local.each_repo():
        local.update_server_info( user, repo )

def main():

    # Parse CLI args.
    parser = ArgumentParser()

    parser.add_argument( '-c', '--config-file', default='gitbacker.ini',
        help='Path to the config file to load.' )
    parser.add_argument( '-q', '--quiet', action='store_true',
        help='Quiet mode.' )
    parser.add_argument( '-v', '--verbose', action='store_true',
        help='Verbose mode.' )
    parser.add_argument( '-w', '--workers', type=int, default=1,
        help='Number of worker processes.' )
    parser.add_argument( '-p', '--pidfile', action='store',
        help='Path to file to write PID of master process to.' )

    subparsers = parser.add_subparsers()
    
    parser_backup = subparsers.add_parser(
        'backup', help='Perform backup of repositories locally.' )

    parser_backup.add_argument( '-s', '--starred-repos', action='store_true',
        help='Backup starred repositories.' )
    parser_backup.add_argument( '-r', '--user-repos', action='store_true',
        help='Backup user repositories.' )
    parser_backup.add_argument( '-m', '--max-size', type=int,
        help='Maximum repo size. Ignore repos larger than in MB.' )
    parser_backup.add_argument( '-g', '--user-gists', action='store_true',
        help='Backup user gists.' )
    parser_backup.add_argument( '-f', '--starred-gists', action='store_true',
        help='Backup authenticated user\'s starred gists.' )
    parser_backup.add_argument( '-t', '--topic', action='store',
        help='Only backup repositories with the given topic attached.' )
    parser_backup.add_argument( '-x', '--redo', action='store_true',
        help='Remove existing repos and re-clone.' )
    parser_backup.add_argument( '-e', '--email', action='store',
        help='Change the e-mail on commits to downloaded repos (implies -x).' )
    parser_backup.add_argument( '-n', '--name', action='store',
        help='Change the name on commits to downloaded repos (implies -x).' )
    parser_backup.add_argument( '-d', '--db', action='store_true',
        help='Store metadata in DB from config.' )

    parser_backup.set_defaults( func=do_backup )

    parser_metaref = subparsers.add_parser( 'metaref' )

    parser_metaref.set_defaults( func=do_metaref )

    args = parser.parse_args()

    if args.quiet:
        logging.basicConfig( level=logging.WARNING )
    elif args.verbose:
        logging.basicConfig( level=logging.DEBUG )
    else:
        logging.basicConfig( level=logging.INFO )
    logger = logging.getLogger( 'main' )

    if 'func' not in args:
        parser.print_help()
        sys.exit( 1 )

    # Load auth config.
    config = ConfigParser()
    config.read( args.config_file )

    notifier = Notifier(
        config.get( 'notify', 'smtp_host' ),
        config.get( 'notify', 'smtp_to' ),
        config.get( 'notify', 'smtp_from' ) )

    # Setup the process management structures.
    msg_queue = multiprocessing.Queue()
    watcher = SigWatcher( notifier, msg_queue, True )
    res_queue = multiprocessing.Queue()

    # Write a PID file if requested.
    if args.pidfile:
        with open( args.pidfile, 'w' ) as pidfile:
            pidfile.write( str( os.getpid() ) )

    try:
        args_arr = vars( args )
        procs = []
        for p in range( 0, args.workers ):
            p_proc = multiprocessing.Process(
                target=args.func, args=(config, notifier, res_queue, msg_queue,
                args.workers, p, args_arr) )
            watcher.add_proc( p_proc )
            procs.append( p_proc )
            p_proc.start()

        # Even if we engage the signal handler here, the children still seem
        # to get their own copy? But the msg_queue system seems to work cleanly
        # regardless, so w/e.
        signal.signal( signal.SIGINT, watcher.handle )
        signal.signal( signal.SIGTERM, watcher.handle )

        for p in procs:
            p.join()

        repos_count = 0
        err_msgs = []
        while not res_queue.empty():
            msg = res_queue.get()
            if MP_MSG_COUNT == msg[0]:
                repos_count += msg[1]
            elif MP_MSG_ERRS == msg[0]:
                err_msgs.append( msg[1] )

        if 0 < len( err_msgs ):
            notifier.send(
                '[gitbacker] Errors during run',
                '\n\n'.join( err_msgs ) )

        notifier.send(
            '[gitbacker] Backed up {} repos OK'.format( repos_count ),
            'Backed up {} repos OK'.format( repos_count ) )

    except Exception as e:
        notifier.send_exc(
            '[gitbacker] Uncaught ERROR during gitbacker', str( e ) )
        logger.exception( e )

if '__main__' == __name__:
    main()

