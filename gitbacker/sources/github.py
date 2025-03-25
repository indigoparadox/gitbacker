
import os
import re
import logging
import requests

from .. import GitBackupFailedException

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

    def get_own_user_repos( self, username ):
        response = self._call_api( 'user/repos' )
        for repo in self._get_paged( response ):
            repo_full = '{}/{}'.format( repo['owner']['login'], repo['name'] )
            if repo_full in self.skip_repos:
                self.logger.info( 'skipping repo %s...', repo_full )
                continue
            
            yield GitHubRepo( repo, self.topic_filter, self.max_size )

    def get_own_starred_gists( self, username ):
        response = self._call_api( 'gists/starred' )
        for gist in self._get_paged( response ):
            yield GitHubGist( gist, self.topic_filter, self.max_size )

    def get_user_gists( self, username ):
        user = self.get_user( username )
        response = self._call_api( 'users/{}/gists'.format( username ) )
        for gist in self._get_paged( response ):
            yield GitHubGist( gist, self.topic_filter, self.max_size )


