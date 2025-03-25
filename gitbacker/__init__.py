
class GitBackupFailedException( Exception ):
    def __init__( self, msg, op, **kwargs ):
        super( GitBackupFailedException, self ).__init__(
            'ERROR during {}'.format( op ) )
        self.op = op
        self.msg = msg
        self.func_args = {}
        for arg in kwargs:
            self.func_args[arg] = kwargs[arg]

