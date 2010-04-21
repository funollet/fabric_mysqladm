# -*- coding: utf-8 -*-

from fabric.api import *
from fabric.contrib.files import *


# Default permissions for grant().
env.mysqladm_perms = """SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, INDEX,
    ALTER, CREATE TEMPORARY TABLES, LOCK TABLES"""



def __generate_password (length=8):
    """Returns a random password of the given 'length' (default=8).
    """

    import random

    ascii_letters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
    digits = '0123456789'
    allchars = ascii_letters + digits
    password = ''
    generator = random.Random()
    for ___ in range(0, length):
        password += generator.choice(allchars)

    return password



def create_db (mysql_db):
    """Create a Mysql db on the remote host.
    
    mysql_db:     name of the db
    """
    run('mysqladmin create %s' % mysql_db)


def drop_db (mysql_db):
    """Drop a Mysql db on the remote host.

    mysql_db:     name of the db
    """
    run('mysqladmin -f drop %s' % mysql_db)


def drop_user (mysql_user):
    """Drop a Mysql user on the remote host.

    mysql_user:     name of the user
    """
    run("""mysql -e "DROP USER %s ;" """ % mysql_user)


def grant (mysql_db, mysql_user, mysql_client='localhost'):
    """Grant Mysql permissions on the remote host.

Parameters:
    mysql_db:     name of the db
    mysql_user:   name for the db user
    [mysql_client]: [localhost] allow connections from this host
    
Requires:
    mysqladm_perms
    """
    
    require('mysqladm_perms')
    cmd = """mysql -e "GRANT %s ON %s.* TO '%s'@'%s' ;" """
    run(cmd % (env.mysqladm_perms, mysql_db, mysql_user, mysql_client))


#TODO: validate username < 16 chars, no underscores
def set_password (mysql_user, mysql_password, mysql_client='localhost'):
    """Set new password for a Mysql user.

Parameters:
    mysql_user:     name for the db user
    mysql_password: new password for the mysql_user
    [mysql_client]: [localhost] allow connections from this host
    """

    cmd = """mysql -e "SET PASSWORD FOR '%s'@'%s' = PASSWORD('%s') ;" """
    run(cmd % (mysql_user, mysql_client, mysql_password))


def copy_db (src, dest):
    """Copy a Mysql db into a new one (same server).

Parameters:
    src:  name of original DB
    dest: name of destiation DB
    """
    create_db(dest)
    run('mysqldump %s | mysql %s' % (src, dest))


def migrate_db (mysql_db, dest_host, ssh_key='migrakey'):
    """Copy a DB into another Mysql server with an SSH-tunnel.

Parameters:
    mysql_db
    dest_host
    ssh_key
    """
    cmd = 'mysqldump %s | ssh -i ~/.ssh/%s %s mysql %s'
    run(cmd % (mysql_db, ssh_key, dest_host, mysql_db))



def make_cnf(cnf_fname, mysql_user, mysql_server, mysql_password):
    """Generate a Mysql .cnf file.

Parameters:

    cnf_fname:      destination filename on the server
    mysql_user:     username for Mysql
    mysql_server:   host where Mysql runs
    mysql_password: password for the new db user
    """

    context = (mysql_user, mysql_password, mysql_server)
    # Upload config file.
    upload_template('my.cnf.tmpl', cnf_fname, context=context)
    # Restrict permissions.
    run('chmod 600 %s' % cnf_fname)
    # Same owner as home directory.
    home_dir = os.path.normpath(os.path.join(cnf_fname, '..'))
    user_group = run('stat -c %%u:%%g %s' % home_dir)
    run('chown %s %s' % (user_group, cnf_fname))




def new_db (mysql_db, local_user, mysql_client=None):
    """New Mysql DB with a new user/password.

Put .my.cnf on <mysql_client>, at ~/.my.cnf.<mysql_db>
    
    mysql_db:       name of the new database
    local_user:     username at the server 
    mysql_client:   host where the Mysql client will run (FQDN) [mysql_server]
                    Not needed if Mysql client and server run on the same host.
    """

    # Choose username, password, client and server.
    mysql_user = '%s_u' % mysql_db
    if len(mysql_user) > 15:
        mysql_user = '%s' % mysql_db

    mysql_password = __generate_password(16)

    mysql_server = env.host

    if not mysql_client:
        # Server and client on the same host.
        fqdn_client = mysql_server      # upload .my.cnf to this host
        connect_from = 'localhost'      # accept Mysql connections from this host
    else:
        fqdn_client = mysql_client      # upload .my.cnf to this host
        connect_from = mysql_client     # accept Mysql connections from this host

    cnf_fname = '/home/%s/.my.cnf.%s' % (local_user, mysql_db)


    create_db(mysql_db)
    grant(mysql_db, mysql_user, connect_from)
    set_password(mysql_user, mysql_password, connect_from)

    # Hack to change the host where next task is run (Fabric-0.9).
    env.user, env.port, env.host = ('root', '22', fqdn_client)
    env.host_string = '%(user)s@%(host)s:%(port)s' % env

    make_cnf(cnf_fname, mysql_user, mysql_server, mysql_password)

