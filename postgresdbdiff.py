#!/usr/bin/env python

# If you are reading this code and thinking: why this file have not been
# split into smaller and easier to read modules? The answer is quite simple:
# I want users to be able just copy/paste this file and run it
import argparse
import difflib
import os.path
import subprocess
import sys


class PsqlOptions:
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

def check_args(psql_options):
    try:
        out = db_out(psql_options, "SELECT 42", stderr=None)
    except subprocess.CalledProcessError:
        raise argparse.ArgumentTypeError(
            'Can not access DB using psql. Probably it does not exists.'
        )

    if '42' not in out:
        raise argparse.ArgumentTypeError(
            'Unknown problem executing SQL statements using psql. Aborting.'
        )


def check_diff_directory(name):
    path = os.path.join(name)
    if not os.path.exists(path):
        return name

    if not os.path.isdir(path):
        raise argparse.ArgumentTypeError('It is not a directory')

    if os.listdir(path):
        raise argparse.ArgumentTypeError('Directory must be empty')

    return name


def parser_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument('--db1', help='First DB name', required=True)
    parser.add_argument('--host1', help='First host name', required=True)
    parser.add_argument('--port1', help='First host port used', required=True)
    parser.add_argument('--user1', help='First host username', required=True)
    parser.add_argument('--pass1', help='First host password', required=True)
    parser.add_argument('--db2', help='Second DB name', required=True)
    parser.add_argument('--host2', help='Second host name', required=True)
    parser.add_argument('--port2', help='Second host port used', required=True)
    parser.add_argument('--user2', help='Second host username', required=True)
    parser.add_argument('--pass2', help='Second host password', required=True)
    parser.add_argument('--diff-folder',
                        help='Directory to output diffs',
                        required=False)
    parser.add_argument('--rowcount',
                        help='Compare tables row count',
                        action='store_true')
    parser.add_argument('--tables-only',
                        help='only compare tables',
                        action='store_true')

    return parser.parse_args()


def db_out(psql_options, cmd, stderr=subprocess.STDOUT, extra_opts=''):
    return subprocess.check_output(
        f"PGPASSWORD='{psql_options.password}' psql -h '{psql_options.host}' "
        f"-p '{psql_options.port}' -U '{psql_options.user}' -d '{psql_options.database}' "
        f"{extra_opts} -c '{cmd}'", shell=True, stderr=stderr
    ).decode('utf-8')


def get_table_rowcount(psql_options, table_name, stderr=subprocess.STDOUT):
    cmd = 'select count(1) from "{}";'.format(table_name)
    output = db_out(psql_options, cmd, extra_opts='--quiet --tuples-only')
    return int(output.strip())


def get_db_tables(psql_options):
    tables = set()
    for line in db_out(psql_options, '\\dt').splitlines():
        elems = line.split()
        if line and elems[0] == 'public':
            tables.add(elems[2])
    return tables


def get_db_views(psql_options):
    views = set()
    for line in db_out(psql_options, '\\dv').splitlines():
        elems = line.split()
        if line and elems[0] == 'public':
            views.add(elems[2])
    return views


def get_db_mat_views(psql_options):
    views = set()
    for line in db_out(psql_options, '\\dmv').splitlines():
        elems = line.split()
        if line and elems[0] == 'public':
            views.add(elems[2])
    return views


def get_table_definition(psql_options, table_name):
    lines = db_out(psql_options, '\\d "{}"'.format(table_name)).splitlines()
    lines = [x for x in lines if x.strip()]

    columns_range = [None, None]
    indexes_range = [None, None]
    check_constr_range = [None, None]
    foreign_constr_range = [None, None]
    process_constr_range = [None, None]

    S_START = 1
    S_COLUMNS = 2
    S_INDEXES = 3
    S_CHECK_CONSTR = 4
    S_FOREIGN_CONSTR = 5
    S_REFERENCES = 6
    S_END = 7

    def replace_with_sorted(lines, a, b):
        if a is None or b is None:
            return lines
        return lines[:a] + sorted(lines[a:b]) + lines[b:]

    def get_after_columns_state(x):
        if x == 'Indexes:':
            return S_INDEXES
        elif x == 'Check constraints:':
            return S_CHECK_CONSTR
        elif x == 'Foreign-key constraints:':
            return S_FOREIGN_CONSTR
        elif x == 'Referenced by:':
            return S_REFERENCES
        return S_END

    def update_range(line_range, i):
        if line_range[0] is None:
            line_range[0] = i
            line_range[1] = i + 1
        else:
            line_range[1] = i + 1

    def process_start(i, x):
        if x[0:2] == '--':
            return S_COLUMNS
        return S_START

    def process_columns(i, x):
        if x[0] != ' ':
            return get_after_columns_state(x)
        update_range(columns_range, i)
        return S_COLUMNS

    def process_indexes(i, x):
        if x[0] != ' ':
            return get_after_columns_state(x)
        update_range(indexes_range, i)
        return S_INDEXES

    def process_check_constr(i, x):
        if x[0] != ' ':
            return get_after_columns_state(x)
        update_range(check_constr_range, i)
        return S_CHECK_CONSTR

    def process_foreign_constr(i, x):
        if x[0] != ' ':
            return get_after_columns_state(x)
        update_range(foreign_constr_range, i)
        return S_FOREIGN_CONSTR

    def process_references(i, x):
        if x[0] != ' ':
            return get_after_columns_state(x)
        update_range(process_constr_range, i)
        return S_REFERENCES

    def process_end(i, x):
        return S_END

    processes = {
        S_START: process_start,
        S_COLUMNS: process_columns,
        S_INDEXES: process_indexes,
        S_CHECK_CONSTR: process_check_constr,
        S_FOREIGN_CONSTR: process_foreign_constr,
        S_REFERENCES: process_references,
        S_END: process_end,
    }

    state = S_START
    for i, x in enumerate(lines):
        state = processes[state](i, x)

    lines = replace_with_sorted(lines, *columns_range)
    lines = replace_with_sorted(lines, *indexes_range)
    lines = replace_with_sorted(lines, *check_constr_range)
    lines = replace_with_sorted(lines, *foreign_constr_range)
    lines = replace_with_sorted(lines, *process_constr_range)
    return '\n'.join(lines)


def compare_number_of_items(db1_items, db2_items, items_name):
    if db1_items != db2_items:
        additional_db1 = db1_items - db2_items
        additional_db2 = db2_items - db1_items

        if additional_db1:
            print(
                '{}: additional in first db\n'.format(items_name)
            )
            for t in additional_db1:
                print('\t{}\n'.format(t))
            print('\n')

        if additional_db2:
            print(
                '{}: additional in second db\n'.format(items_name)
            )
            for t in additional_db2:
                print('\t{}\n'.format(t))
            print('\n')


# TODO: Using same function to compare tables and views. It is not very suited
# for views. But I do not see any clear way to have cleaner interface
def compare_each_table(diff_folder, db1_tables, db2_tables, psql_options1, psql_options2 ,items_name, rowcount=False):
    not_matching_tables = []
    not_matching_rowcount = []

    for t in sorted(db1_tables & db2_tables):
        t1 = get_table_definition(psql_options1, t)
        t2 = get_table_definition(psql_options2, t)
        if t1 != t2:
            not_matching_tables.append(t)

            diff = difflib.unified_diff(
                [x + '\n' for x in t1.splitlines()],
                [x + '\n' for x in t2.splitlines()],
                '{}.{}.{}'.format(items_name, psql_options1.database, t),
                '{}.{}.{}'.format(items_name, psql_options2.database, t),
                n=sys.maxsize
            )

            if diff_folder:
                if not os.path.exists(diff_folder):
                    os.mkdir(diff_folder)
                filepath = os.path.join(
                    diff_folder, '{}.diff'.format(t)
                )
                with open(filepath, 'w') as f:
                    for diff_line in diff:
                        f.write(diff_line)

        elif rowcount:
            t1_rowcount = get_table_rowcount(psql_options1, t)
            t2_rowcount = get_table_rowcount(psql_options2, t)
            if t1_rowcount != t2_rowcount:
                not_matching_rowcount.append('{} ({} != {})'.format(t, t1_rowcount, t2_rowcount))

    if not_matching_tables:
        print('{}: not matching\n'.format(items_name))
        for t in not_matching_tables:
            print('\t{}\n'.format(t))
        print('\n')

    if not_matching_rowcount:
        print('{}: not matching rowcount\n'.format(items_name))
        for t in not_matching_rowcount:
            print('\t{}\n'.format(t))
        print('\n')


def main():
    options = parser_arguments()

    db1_options = PsqlOptions(options.host1, options.port1, options.user1, options.pass1, options.db1)
    db2_options = PsqlOptions(options.host2, options.port2, options.user2, options.pass2, options.db2)

    check_args(db1_options)
    check_args(db2_options)

    db1_tables = get_db_tables(db1_options)
    db2_tables = get_db_tables(db2_options)

    compare_number_of_items(db1_tables, db2_tables, 'TABLES')
    compare_each_table(options.diff_folder, db1_tables, db2_tables, db1_options, db2_options, 'TABLES', options.rowcount)

    if options.tables_only:
        return

    db1_views = get_db_views(db1_options)
    db2_views = get_db_views(db2_options)
    compare_number_of_items(db1_views, db2_views, 'VIEWS')
    compare_each_table(options.diff_folder, db1_views, db2_views, db1_options, db2_options, 'VIEWS', options.rowcount)

    db1_views = get_db_mat_views(db1_options)
    db2_views = get_db_mat_views(db2_options)
    compare_number_of_items(db1_views, db2_views, 'MATERIALIZED VIEWS')
    compare_each_table(options.diff_folder, db1_views, db2_views, db1_options, db2_options, 'MATERIALIZED VIEWS', options.rowcount)


if __name__ == "__main__":
    main()
