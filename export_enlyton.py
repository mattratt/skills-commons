import sys
import os
import json
import os.path
from skills_commons_db import get_connection


host = sys.argv[1]
dbname = sys.argv[2]
user = sys.argv[3]
out_path_root = sys.argv[4]
conn = get_connection(host, dbname, user)


curs = conn.cursor()
sql = "SELECT course_id, file_name, ext, text FROM files ORDER BY course_id, file_name"
# sql += " LIMIT 1000"
curs.execute(sql)

course_id_prev = None
out_dir = None
temp_dirs = set()
course_count = 0
file_num = 0

# read old stuff, pick where we left off
courses_done = set()
for d in os.listdir(out_path_root):
    courses_done.add(d)
    for f in os.listdir(os.path.join(out_path_root, d)):
        num = int(f[:-5])
        if (num + 1) > file_num:
            file_num = num + 1
print "already processed courses < ", max([ int(c) for c in courses_done ])
print "next file num is ", file_num

for i, row in enumerate(curs.fetchall()):
    if i % 10000 == 0:
        print "\t", i, "/", curs.rowcount

    course_id_raw, file_name, ext, text = row
    course_id = course_id_raw[12:]  # strip off prefix, hdl:taaccct/13114

    if course_id in courses_done:
        continue

    # little hack to deal with the captured temp directory names
    if (not ext) and (file_name.endswith('/')):  # i17c52d373cbeed8a22efc936bc0afc25/
        temp_dirs.add(file_name)
        continue

    if course_id != course_id_prev:
        course_count += 1
        out_dir = os.path.join(out_path_root, course_id)
        os.mkdir(out_dir)
        course_id_prev = course_id
        temp_dirs = set()
        # file_num = 0
        print "\nnew course: {} ({})".format(course_id, course_count)

    for temp_dir in temp_dirs:
        if file_name.startswith(temp_dir):
            _, file_name = file_name.split(temp_dir, 1)
            break

    # file_name_clean = file_name.replace(' ', '_').replace('/', '-')
    # with open(os.path.join(out_dir, file_name_clean), 'w') as out_file:
    with open(os.path.join(out_dir, "{}.json".format(file_num)), 'w') as out_file:
        attrs = { 'course_id': course_id, 'file_name': file_name, 'ext': ext, 'text': text }
        json.dump(attrs, out_file)
    # print "\tfile", file_name_clean

    file_num += 1










