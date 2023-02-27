import os, sys
from datetime import datetime
from optparse import OptionParser


class MySQLBackup:

    DB_NAME = os.environ.get('DB_NAME')

    if DB_NAME is None:
        raise Exception('Export the database name in the system, or set up env in docker-compose.yml')

    _LOGIN_INFO = (
        os.environ.get('USERNAME', 'root'),
        os.environ.get('PASSWORD', 'root')
    )

    _DATABASE_INFO = dict()
    if ',' in DB_NAME:
        for db in DB_NAME.split(','):
            _DATABASE_INFO.update({db: _LOGIN_INFO})
    else:
        _DATABASE_INFO = {DB_NAME: _LOGIN_INFO}

    _ROOT = '/backup'

    _CLI_BACKUP_DATABASE = 'mysqldump --default-character-set=utf8 -u {usr} -p{pwd} {db} > {pathname}'
    _CLI_BACKUP_TABLE = 'mysqldump --default-character-set=utf8 -u {usr} -p{pwd} {db} {tb} > {pathname}'

    _CLI_REBUILD_DB = 'mysql -u {usr} -p{pwd} {db} < {pathname}'

    @staticmethod
    def _get_args():
        option = OptionParser()
        option.add_option('-d', '--database', action='store', type='string', dest='database', default=None)
        option.add_option('-t', '--table', action='store', type='string', dest='table', default=None)
        option.add_option('-p', '--path', action='store', type='string', dest='path', default=None)
        option.add_option('-b', '--backup', action='store_true', dest='is_backup', default=False)
        option.add_option('-r', '--rebuild', action='store_true', dest='is_rebuild', default=False)
        return option.parse_args()[0]

    @staticmethod
    def _get_now(format_=None):
        return datetime.now().strftime(format_ or '%Y_%m_%d_%H_%M_%S')

    @classmethod
    def _get_databases(cls, args):
        if not args.database:
            return cls._DATABASE_INFO
        if args.database in cls._DATABASE_INFO:
            return [args.database]
        if ',' in args.database:
            databases = {_ for _ in args.database.split(',') if _.strip() in cls._DATABASE_INFO}
            return list(databases)
        raise ValueError('Cannot parse <-d, --database> database')

    @classmethod
    def _get_tables(cls, args):
        if not args.table:
            return None
        if ',' in args.table:
            tables = {_.strip() for _ in args.table.split(',')}
            return list(tables)
        return [args.table]

    @classmethod
    def _get_backup_path(cls, args):
        if not args.path:
            path = '{root}/sql_backup_{now}'.format(root=cls._ROOT, now=cls._get_now())
            return path
        return args.path

    @classmethod
    def _get_rebuild_path(cls, args):
        if not args.path:
            path_list = [_ for _ in os.listdir(cls._ROOT) if _.startswith('sql_backup_')]
            latest_backup = sorted(path_list, reverse=True)[0] if path_list else None
            return '{root}/{path}'.format(root=cls._ROOT, path=latest_backup)
        return '{root}/{path}'.format(root=cls._ROOT, path=args.path)

    @classmethod
    def _get_login_info(cls, database):
        return cls._DATABASE_INFO.get(database)

    @classmethod
    def _backup_database(cls, args):
        action = 'BACKUP'
        databases = cls._get_databases(args)
        backup_path = cls._get_backup_path(args)
        os.makedirs(backup_path, exist_ok=True)
        for database in databases:
            username, password = cls._get_login_info(database=database)
            pathname = '{path}/{db}.sql'.format(path=backup_path, db=database)
            print('[{action:8}] [ * Proceed Database: {db}]'.format(action=action, db=database))
            os.system(cls._CLI_BACKUP_DATABASE.format(usr=username, pwd=password, db=database, pathname=pathname))
            print('[{action:8}] [ * Finished database: {db}]'.format(action=action, db=database))

    @classmethod
    def _backup_table(cls, args):
        action = 'BACKUP'
        databases = cls._get_databases(args)
        tables = cls._get_tables(args)
        backup_path = cls._get_backup_path(args)
        os.makedirs(backup_path, exist_ok=True)
        for database in databases:
            username, password = cls._get_login_info(database=database)
            for table in tables:
                pathname = '{path}/{db}_{tb}.sql'.format(path=backup_path, db=database, tb=table)
                print('[{action:8}] [ * Proceed Table: {db}.{tb}]'.format(action=action, db=database, tb=table))
                os.system(
                    cls._CLI_BACKUP_TABLE.format( usr=username, pwd=password, db=database, tb=table, pathname=pathname)
                )
                print('[{action:8}] [ * Finished Table: {db}.{tb}]'.format(action=action, db=database, tb=table))

    @classmethod
    def _rebuild_database(cls, args):
        action = 'REBUILD'
        databases = cls._get_databases(args)
        rebuild_path = cls._get_rebuild_path(args)
        for database in databases:
            username, password = cls._get_login_info(database=database)
            pathname = '{path}/{db}.sql'.format(path=rebuild_path, db=database)
            if not os.path.isfile(pathname):
                print('[{info:8}] [ * File <{path}> is not exist!]'.format(info='WARNING', path=pathname))
                continue

            print('[{action:8}] [ * Proceed {db}]'.format(action=action, db=database))
            os.system(cls._CLI_REBUILD_DB.format(usr=username, pwd=password, db=database, pathname=pathname))
            print('[{action:8}] [ * Finished {db}]'.format(action=action, db=database))

    @classmethod
    def _rebuild_table(cls, args):
        action = 'REBUILD'
        databases = cls._get_databases(args)
        tables = cls._get_tables(args)
        rebuild_path = cls._get_rebuild_path(args)
        for database in databases:
            username, password = cls._get_login_info(database=database)
            for table in tables:
                pathname = '{path}/{db}_{tb}.sql'.format(path=rebuild_path, db=database, tb=table)
                if not os.path.isfile(pathname):
                    print('[{info:8}] [ * File <{path}> is not exist!]'.format(info='WARNING', path=pathname))
                    continue

                print('[{action:8}] [ * Proceed {db}]'.format(action=action, db=database))
                os.system(cls._CLI_REBUILD_DB.format(usr=username, pwd=password, db=database, pathname=pathname))
                print('[{action:8}] [ * Finished {db}]'.format(action=action, db=database))

    @classmethod
    def run(cls):
        args = cls._get_args()
        if not (args.is_backup or args.is_rebuild):
            print('-b [--backup] or -r [--rebuild] either option is required!')
            exit()
        if args.is_rebuild:
            dbs = ', '.join(cls._get_databases(args))
            print('[{info:8}] [ * Rebuild {dbs} (yes/no)?]'.format(info='WARNING', dbs=dbs))
            if input('>>> ').lower() != 'yes':
                print('Program has been terminated.')
                exit()
        if args.is_backup:
            if args.table:
                cls._backup_table(args)
            else:
                cls._backup_database(args)
        if args.is_rebuild:
            if args.table:
                cls._rebuild_table(args)
            else:
                cls._rebuild_database(args)


if __name__ == '__main__':
    MySQLBackup.run()

