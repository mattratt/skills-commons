import csv
import sys
import argparse
import getpass
import os.path
import psycopg2 as db
import skills_commons as sc


log = sc.get_log(__name__)
csv.field_size_limit(sys.maxsize)


def create_all_tables(conn, overwrite=False):
    curs = conn.cursor()
    if overwrite:
        drop_table(curs, 'courses')
        drop_table(curs, 'files')

    course_col_tups = [('course_id', 'VARCHAR(128)')]
    for field_name in sc.COURSE_FIELDS:
        col_name = field_name.replace(' ', '_')
        col_type = 'VARCHAR(256)'

        # insert exceptions here:
        if field_name in {'date accessioned', 'date available'}:
            col_type = 'DATE'
        elif field_name in {'description abstract', 'description sponsorship', 'subject',
                            'courseNote', 'qualityNote', 'rightsHolder', 'SMQuality'}:
            col_type = 'TEXT'
        course_col_tups.append((col_name, col_type))
    create_table(curs, 'courses', course_col_tups)

    file_col_tups = [
        ('course_id', 'VARCHAR(128)'),
        ('file_name', 'VARCHAR(512)'),
        ('ext', 'VARCHAR(128)'),
        ('text', 'TEXT')
    ]
    create_table(curs, 'files', file_col_tups)


def create_table(curs, table_name, col_tups):
    sql = "CREATE TABLE " + table_name + " ("
    sql += ", ".join([nam + " " + typ for nam, typ in col_tups])
    sql += ")"
    curs.execute(sql)


def drop_table(curs, table_name):
    sql = "DROP TABLE IF EXISTS " + table_name
    curs.execute(sql)


# def insert_row(curs, table_name, col__val):
#     col_val_tups = col__val.items()
#     cols = [ t[0] for t in col_val_tups ]
#     vals = [ t[1] for t in col_val_tups ]
#     sql = "INSERT INTO " + table_name + " ("
#     sql += ", ".join(cols)
#     sql += ") VALUES ("
#     sql += ", ".join(["%s"]*len(vals))
#     sql += ")"
#     try:
#         curs.execute(sql, vals)
#     except Exception as err:
#         log.warning("error inserting record: {} {}".format(sql, vals))
#         raise err


def insert_course_rows_from_file(curs, course_info_path):
    log.debug("inserting courses from {}".format(course_info_path))
    sql = "INSERT INTO courses VALUES (%s, "
    sql += ", ".join(['%s'] * len(sc.COURSE_FIELDS))
    sql += ")"
    insert_count = 0
    with open(course_info_path, 'rb') as course_file:
        course_reader = csv.reader(course_file, delimiter='\t', quotechar='"')
        for row in course_reader:
            #zzz this is a hack, need to figure out which fields too long!
            try:
                curs.execute(sql, [ r if (r != '') else None for r in row ])
            except db.DataError as err:
                for i, r in enumerate(row):
                    col = sc.COURSE_FIELDS[i-1] if (i>0) else "course_id"
                    log.debug("\t({})\t{}\t{}".format(len(r), col, r))
                raise
            insert_count += 1
    return insert_count


def insert_course_row(curs, course_id, field__val):
    params = [course_id]
    for field in sc.COURSE_FIELDS:
        val = field__val.get(field)
        if val is None:
            params.append(None)
        else:
            params.append(' '.join(val.split()))

    sql = "INSERT INTO courses VALUES (%s, "
    sql += ", ".join(['%s']*len(sc.COURSE_FIELDS))
    sql += ")"
    curs.execute(sql, params)


def insert_file_rows_from_file(conn, file_info_path):
    curs = conn.cursor()
    log.debug("inserting files from {}".format(file_info_path))
    sql = "INSERT INTO files VALUES (%s, %s, %s, %s)"
    insert_count = 0
    with open(file_info_path, 'rb') as infile:
        reader = csv.reader(infile, delimiter='\t', quotechar='"')
        for i, row in enumerate(reader):
            if i % 10000 == 0:
                log.debug("\t{}".format(i))
            try:
                vals = [ r if (r != '') else None for r in row ]
                curs.execute(sql, vals)
            except db.DataError as err:
                log.debug("could not insert file row: {}".format(err))
                continue
            except db.InternalError as err:
                log.debug("could not insert file row: {}".format(err))
                conn.rollback()
                continue

            conn.commit()
            insert_count += 1
    return insert_count


def insert_file_row(curs, course_id, file_name, text):
    # course_id, file_name, ext, text
    _, ext = os.path.splitext(file_name.lower())
    params = [course_id, file_name, ext, text]
    sql = "INSERT INTO files VALUES (%s, %s, %s, %s)"
    curs.execute(sql, params)


def get_connection(host, dbname, user=None):
    if user is None:
        user = raw_input("db username: ")
    passw = getpass.getpass("db password: ")
    connstr = "host='{}' dbname={} user={} password='{}'".format(host, dbname, user, passw)
    return  db.connect(connstr)


def get_processed_course_ids(curs):
    curs.execute("SELECT course_id FROM courses")
    return { row[0] for row in curs.fetchall() }


#####################################

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Import Skills Commons data into relational db')
    parser.add_argument('host')
    parser.add_argument('db')  # skillscomm2
    parser.add_argument('user', help='username for db')
    parser.add_argument('--courses_path', help='path of course data tsv')
    parser.add_argument('--files_path', help='path of files data tsv')
    parser.add_argument('--create_db', help='erase and create new tables', action='store_true')
    args = parser.parse_args()

    log.info("connecting to db")
    conn = get_connection(args.host, args.db, args.user)

    if args.create_db:
        log.info("creating tables")
        create_all_tables(conn, overwrite=True)

    log.info("loading infiles")
    curs = conn.cursor()

    # if we supply paths to data files, load those
    if args.courses_path:
        count = insert_course_rows_from_file(curs, args.courses_path)
        log.info("loaded {} courses".format(count))
    if args.files_path:
        count = insert_file_rows_from_file(conn, args.files_path)
        log.info("loaded {} files".format(count))
    conn.commit()

    # otherwise, go direct to the source
    # parser = argparse.ArgumentParser(description='Pull down course info from Skills Commons')
    # parser.add_argument('datadir', help='destination for tsv files produced')
    # parser.add_argument('--max_results', type=int, default=sys.maxint,
    #                     help='max number of courses to download')
    # parser.add_argument('--rpp', type=int, default=300,
    #                     help='results per page for Skills Commons API')
    # parser.add_argument('--cont', action='store_true',
    #                     help='continue (skip courses already proessed)')
    # args = parser.parse_args()

    results = sc.get_course_listing('online', sc.SEARCH_RPP, sys.maxint)
    print "got {} results".format(len(results))

    processed = get_processed_course_ids(curs)

    for i, (course_id, course_fields, course_files) in enumerate(results):
        log.debug("course {}/{}".format(i, len(results) - 1))

        if course_id in processed:
            log.debug("skipping previously processed course {}".format(course_id))
            continue

        file_name__text = {}
        for j, (file_name, file_label, file_type, file_url) in enumerate(course_files):
            log.debug("course {}/{}  file {}/{}".format(i, len(results) - 1,
                                                        j, len(course_files) - 1))
            file_name__text.update(sc.get_text_url(file_url))

        # for file_name, text in sorted(file_name__text.items()):
        #     try:
        #         log.debug("\t{} ({})".format(file_name.encode('ascii', 'ignore'),
        #                                      len(text) if text is not None else 0))
        #     except Exception as err:
        #         continue

        insert_course_row(curs, course_id, course_fields)
        for file_name, text in file_name__text.items():
            try:
                insert_file_row(curs, course_id, file_name, text)
            except (ValueError, db.DataError) as err:
                log.debug("error inserting {}, {}: {}".format(course_id, file_name, err))
            conn.commit()

        log.debug("\n")




    # SQL QUERIES:
    # file type distrib overall for all files
    # number of courses with at least one type x, y, z, ...
    # distrib of text amount for each course


    # PYTHON text:
    # file type distrib for top-level files

    # PYTHON graphical:
    # distrib of overall file count for each course
    # distrib of count of each file type for each course

    # some notion of topics of text for each course






