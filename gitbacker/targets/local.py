
import os
import shutil
import logging

from git import Repo, Remote
from git.exc import GitCommandError

from .. import GitBackupFailedException

class LocalRepo( object ):

    def __init__( self, root, db_conn ):

        self._root = root
        self._db_conn = db_conn
        self.logger = logging.getLogger( 'localrepo' )

    def _try_repeat( self, try_count, func, **kwargs ):
        while 0 < try_count:
            try:
                func( **kwargs )
                # If successful, don't loop.
                try_count = 0
            except GitBackupFailedException as e:
                raise e
            except Exception as e:
                if PATTERN_REMOTE_REF.search( str( e ) ):
                    # TODO: Maybe use a different logger?
                    self.logger.debug( '{}: {}'.format(
                        kwargs['repo_name'], e ) )
                    # If this is a dead branch, just move on.
                    self.logger.info( 'skipping deleted branch...' )
                    try_count = 0
                else:
                    self.logger.error( 'error during %s; retrying...', func )
                    try_count -= 1
                    if 0 >= try_count:
                        # Just give up for now.
                        raise GitBackupFailedException(
                            str( e ), func.__name__, **kwargs )

    def get_root( self ):
        return self._root

    def get_path( self, repo_name, owner_name=None ):
        if owner_name:
            return os.path.join(
                self._root, owner_name,
                    '{}.git'.format( repo_name ) if not \
                        repo_name.endswith( '.git' ) else repo_name )
        else:
            return os.path.join( self._root,
                '{}.git'.format( repo_name ) if not \
                    repo_name.endswith( '.git' ) else repo_name )

    def each_repo( self ):
        for user in os.listdir( self._root ):
            user_dir = os.path.join( self._root, user )
            if not os.path.isdir( user_dir ):
                self.logger.warning( 'invalid user dir: %s', user_dir )
                continue

            for repo in os.listdir( user_dir ):
                repo_dir = os.path.join( self._root, user, repo )
                if not os.path.isdir( user_dir ):
                    self.logger.warning( 'invalid repo dir: %s', repo_dir )
                    continue
                if not repo_dir.endswith( '.git' ):
                    self.logger.warning( 'dir %s not a repo?', repo_dir )
                    continue
                yield repo_dir, user, repo

    def fetch_branch( self, owner_name, repo_name, remote, branch ):
        self.logger.info( 'checking {}/{} branch: {}'.format(
            owner_name, repo_name, branch ) )
        repo_dir = self.get_path( repo_name, owner_name )
        remote.fetch( branch )

    def fetch_all_branches( self, owner_name, repo_name ):
        repo_dir = self.get_path( repo_name, owner_name )
        repo = Repo( repo_dir )
        for remote in repo.remotes:
            try:
                self.logger.info( 'attempting group branch fetch...' )
                remote.fetch( '*:*' )
            except:
                self.logger.info( 'group branch fetch failed, looping...' )
                branches = []
                try:
                    branches = [b.name for b in repo.branches]
                except UnicodeDecodeError as e:
                    self.logger.error(
                        'could not decode branch name: {}'.format( e ) )
                for remote in repo.remotes:
                    # Try to fetch all new branches, but no refspec?
                    #self._try_repeat( 3, remote.fetch )
                    for branch in branches:
                        self._try_repeat(
                            3, self.fetch_branch,
                            owner_name=owner_name, repo_name=repo_name,
                            remote=remote, branch=branch )

    def update_server_info( self, owner_name, repo_name ):
        self.logger.info( 'updating server info for %s/%s...',
            owner_name, repo_name )
        repo_dir = self.get_path( repo_name, owner_name )
        repo = Repo( repo_dir )
        repo.git.update_server_info()

        with repo.config_writer( 'repository' ) as cfg:
            cfg.set_value( 'gitweb', 'owner', owner_name )

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

    def clone_create( self, owner_name, repo_name, remote_url ):
        repo_dir = self.get_path( repo_name, owner_name )
        self.logger.info( 'creating local repo copy...' )
        # So our stored credentials work.
        remote_url = remote_url.replace( 'git://', 'https://' )
        Repo.clone_from( remote_url, repo_dir, bare=True )
        self.update_server_info( owner_name, repo_name )

    def create_or_update( self, repo_name, remote_url, owner_name=None ):

        repo_dir = self.get_path( repo_name, owner_name )
        if not os.path.exists( repo_dir ):
            try:
                self._try_repeat( 3, self.clone_create,
                    repo_name=repo_name, owner_name=owner_name,
                    remote_url=remote_url )
            except Exception as e:
                if os.path.exists( repo_dir ):
                    self.logger.info( 'removing failed clone %s...', repo_dir )
                    shutil.rmtree( repo_dir )
                raise( e )

        self.logger.info( 'checking all remote repo branches...' )
        self._try_repeat(
            3, self.fetch_all_branches,
            owner_name=owner_name, repo_name=repo_name )
        self.update_server_info( owner_name, repo_name )

