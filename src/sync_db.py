import ConfigParser
import getopt
import os

import MySQLdb
import sys


def sys_db():
    opts, args = getopt.getopt(sys.argv[1:], "f:")
    cfg_file = None
    for op, value in opts:
        if op == "-f":
            cfg_file = value
    
    if not cfg_file:
        print 'need config file'
        exit()
    
    config = ConfigParser.ConfigParser()
    config.read(cfg_file)
    src_db_host = config.get('db_info', 'src_db_host')
    src_db_user = config.get('db_info', 'src_db_user')
    src_db_password = config.get('db_info', 'src_db_password')
    src_db_name = config.get('db_info', 'src_db_name')
    
    target_db_host = config.get('db_info', 'target_db_host')
    target_db_user = config.get('db_info', 'target_db_user')
    target_db_password = config.get('db_info', 'target_db_password')
    target_db_name = config.get('db_info', 'target_db_name')
    
    tables = {}
    for option in config.options('table_info'):
        tables[option] = config.get('table_info', option)
    
    src_conn = MySQLdb.connect(host=src_db_host, user=src_db_user, passwd=src_db_password,
                               db=src_db_name,
                               charset="utf8")
    src_cursor = src_conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    
    target_conn = MySQLdb.connect(host=target_db_host, user=target_db_user,
                                  passwd=target_db_password, db=target_db_name,
                                  charset="utf8")
    target_cursor = target_conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    
    for table in tables:
        key = tables[table]
        sql = 'select max(%s) _key from %s' % (key, table)
        src_cursor.execute(sql)
        src_max_key = src_cursor.fetchall()[0]['_key']
        
        if not src_max_key:
            continue
        
        target_cursor.execute(sql)
        target_max_key = target_cursor.fetchall()[0]['_key']
        
        if not target_max_key:
            target_max_key = 0
        
        sql = '''select * from %s where %s > '%s' ''' % (table, key, target_max_key)
        src_cursor.execute(sql)
        
        for row in src_cursor.fetchall():
            keys = row.keys()
            values = row.values()
            keys = ','.join(keys)
            i_param = "%s," * len(row)
            i_param = i_param[:-1]
            
            sql = '''replace into %s(%s) values(%%s)''' % (table, keys)
            sql %= i_param
            target_cursor.execute(sql, values)
        
        target_conn.commit()
    
    src_cursor.close()
    src_conn.close()
    target_cursor.close()
    target_conn.close()


if '__main__' == __name__:
    sys_db()
